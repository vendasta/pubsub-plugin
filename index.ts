import { Plugin, PluginMeta, PluginEvent, RetryError } from '@posthog/plugin-scaffold'
import { PubSub, Topic } from "@google-cloud/pubsub"

type PubSubPlugin = Plugin<{
    global: {
        pubSubClient: PubSub
        pubSubTopic: Topic
    }
    config: {
        topicId: string
    },
}>

export const setupPlugin: PubSubPlugin['setupPlugin'] = async (meta) => {
    const { global, attachments, config } = meta
    if (!attachments.googleCloudKeyJson) {
        throw new Error('JSON config not provided!')
    }
    if (!config.topicId) {
        throw new Error('Topic ID not provided!')
    }

    try {
        const credentials = JSON.parse(attachments.googleCloudKeyJson.contents.toString())
        global.pubSubClient = new PubSub({
            projectId: credentials['project_id'],
            credentials,
        })
        global.pubSubTopic = global.pubSubClient.topic(config.topicId);

        // topic exists
        await global.pubSubTopic.getMetadata()
    } catch (error) {
        // some other error? abort!
        if (!error.message.includes("NOT_FOUND")) {
            throw new Error(error)
        }
        console.log(`Creating PubSub Topic - ${config.topicId}`)

        try {
            await global.pubSubTopic.create()
        } catch (error) {
            // a different worker already created the table
            if (!error.message.includes('ALREADY_EXISTS')) {
                throw error
            }
        }
    }
}

export async function exportEvents(events: PluginEvent[], { global, config }: PluginMeta<PubSubPlugin>) {
    if (!global.pubSubClient) {
        throw new Error('No PubSub client initialized!')
    }
}