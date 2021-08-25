import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, PluginEvent, RetryError } from '@posthog/plugin-scaffold'
import { PubSub, Topic } from "@google-cloud/pubsub";

type PubSubPlugin = Plugin<{
    global: {
        pubSubClient: PubSub
        pubSubTopic: Topic

        exportEventsBuffer: ReturnType<typeof createBuffer>
        exportEventsToIgnore: Set<string>
        exportEventsWithRetry: (payload: UploadJobPayload, meta: PluginMeta<PubSubPlugin>) => Promise<void>
    }
    config: {
        topicId: string

        exportEventsBufferBytes: string
        exportEventsBufferSeconds: string
        exportEventsBuffer: ReturnType<typeof createBuffer>
        exportEventsToIgnore: string
        exportElementsOnAnyEvent: 'Yes' | 'No'
    },
    jobs: {
        exportEventsWithRetry: UploadJobPayload
    }
}>

interface UploadJobPayload {
    batch: PluginEvent[]
    batchId: number
    retriesPerformedSoFar: number
}

export const setupPlugin: PubSubPlugin['setupPlugin'] = async (meta) => {
    const { global, attachments, config } = meta
    if (!attachments.googleCloudKeyJson) {
        throw new Error('JSON config not provided!')
    }
    if (!config.topicId) {
        throw new Error('Topic ID not provided!')
    }

    const credentials = JSON.parse(attachments.googleCloudKeyJson.contents.toString())
    global.pubSubClient = new PubSub({
        projectId: credentials['project_id'],
        credentials,
    })
    global.pubSubTopic = global.pubSubClient.topic(config.topicId);

    try {
        // topic exists
        await global.pubSubTopic.getMetadata()
    } catch (error) {
        // some other error? abort!
        if (!error.message.includes("NOT_FOUND")) {
            throw new Error(error)
        }
        console.log(`Creating PubSub Topic - ${config.topicId}`)

        try {
            await global.pubSubTopic.create();
        } catch (error) {
            // a different worker already created the table
            if (!error.message.includes('ALREADY_EXISTS')) {
                throw error
            }
        }
    }

    setupBufferExportCode(meta, exportEventsToPubSub)
}

export async function exportEventsToPubSub(events: PluginEvent[], { global, config }: PluginMeta<PubSubPlugin>) {
    if (!global.pubSubClient) {
        throw new Error('No PubSub client initialized!')
    }
    try {
        const messages = events.map((fullEvent) => {
            const {
                event: eventName,
                properties,
                $set,
                $set_once,
                distinct_id,
                team_id,
                site_url,
                now,
                sent_at,
                uuid,
                ..._discard
            } = fullEvent
            const ip = properties?.['$ip'] || fullEvent.ip
            const timestamp = fullEvent.timestamp || properties?.timestamp || now || sent_at
            let ingestedProperties = properties
            let elements = []

            const shouldExportElementsForEvent =
                eventName === '$autocapture' || config.exportElementsOnAnyEvent === 'Yes'

            if (
                shouldExportElementsForEvent &&
                properties &&
                '$elements' in properties &&
                Array.isArray(properties['$elements'])
            ) {
                const { $elements, ...props } = properties
                ingestedProperties = props
                elements = $elements
            }

            const message = {
                eventName,
                distinct_id,
                team_id,
                ip,
                site_url,
                timestamp,
                uuid: uuid!,
                properties: ingestedProperties || {},
                elements: elements || [],
                people_set: $set || {},
                people_set_once: $set_once || {},
            }
            return Buffer.from(JSON.stringify(message));
        })

        const start = Date.now()
        await Promise.all(
            messages.map((dataBuf) =>
                global.pubSubTopic.publish(dataBuf).then((messageId) => {
                    return messageId;
                })
            )
        );
        const end = Date.now() - start

        console.log(
            `Published ${events.length} ${events.length > 1 ? 'events' : 'event'} to Pub/Sub. Took ${
                end / 1000
            } seconds.`
        )
    } catch (error) {
        console.error(
            `Error publishing ${events.length} ${events.length > 1 ? 'events' : 'event'} to Pub/Sub: `,
            error
        )
        throw new RetryError(`Error publishing to Pub/Sub! ${JSON.stringify(error.errors)}`)
    }
}

const setupBufferExportCode = (
    meta: PluginMeta<PubSubPlugin>,
    exportEvents: (events: PluginEvent[], meta: PluginMeta<PubSubPlugin>) => Promise<void>
) => {
    const uploadBytes = Math.max(
        1024 * 1024,
        Math.min(parseInt(meta.config.exportEventsBufferBytes) || 1024 * 1024, 1024 * 1024 * 10)
    )
    const uploadSeconds = Math.max(1, Math.min(parseInt(meta.config.exportEventsBufferSeconds) || 30, 600))

    meta.global.exportEventsToIgnore = new Set(
        meta.config.exportEventsToIgnore
            ? meta.config.exportEventsToIgnore.split(',').map((event) => event.trim())
            : null
    )
    meta.global.exportEventsBuffer = createBuffer({
        limit: uploadBytes,
        timeoutSeconds: uploadSeconds,
        onFlush: async (batch) => {
            const jobPayload = {
                batch,
                batchId: Math.floor(Math.random() * 1000000),
                retriesPerformedSoFar: 0,
            }
            await meta.jobs.exportEventsWithRetry(jobPayload)
        },
    })
    meta.global.exportEventsWithRetry = async (payload: UploadJobPayload, meta: PluginMeta<PubSubPlugin>) => {
        const { jobs } = meta
        try {
            await exportEvents(payload.batch, meta)
        } catch (err) {
            if (err instanceof RetryError) {
                if (payload.retriesPerformedSoFar < 15) {
                    const nextRetrySeconds = 2 ** payload.retriesPerformedSoFar * 3
                    console.log(`Enqueued batch ${payload.batchId} for retry in ${Math.round(nextRetrySeconds)}s`)

                    await jobs
                        .exportEventsWithRetry({ ...payload, retriesPerformedSoFar: payload.retriesPerformedSoFar + 1 })
                        .runIn(nextRetrySeconds, 'seconds')
                } else {
                    console.log(
                        `Dropped batch ${payload.batchId} after retrying ${payload.retriesPerformedSoFar} times`
                    )
                }
            } else {
                throw err
            }
        }
    }
}

export const jobs: PubSubPlugin['jobs'] = {
    exportEventsWithRetry: async (payload, meta) => {
        await meta.global.exportEventsWithRetry(payload, meta)
    },
}

export const onEvent: PubSubPlugin['onEvent'] = (event, { global }) => {
    if (!global.exportEventsToIgnore.has(event.event)) {
        global.exportEventsBuffer.add(event, JSON.stringify(event).length)
    }
}