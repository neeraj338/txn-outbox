package com.cloud.stream.outbox.config

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder
import org.springframework.cloud.stream.binder.ConsumerProperties
import org.springframework.cloud.stream.binder.ProducerProperties
import org.springframework.cloud.stream.provisioning.ConsumerDestination
import org.springframework.cloud.stream.provisioning.ProducerDestination
import org.springframework.cloud.stream.provisioning.ProvisioningProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.core.MessageProducer
import org.springframework.messaging.Message
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.MessageHandler
import java.io.IOException


@Configuration
open class OutboxBinderConfig {

    @Bean
    @ConditionalOnMissingBean
    open fun binderProvisioner(): OutboxMessageBinderProvisioner {
        return OutboxMessageBinderProvisioner()
    }

    @Bean
    @ConditionalOnMissingBean
    open fun messageBinder(
        binderProvisioner: OutboxMessageBinderProvisioner,
    ): OutboxMessageBinder {
        return OutboxMessageBinder(
            provisioningProvider = binderProvisioner,
        )
    }
}

open class OutboxMessageBinder(
    headersToEmbed: List<String> = emptyList(),
    provisioningProvider: OutboxMessageBinderProvisioner,

) :
    AbstractMessageChannelBinder<ConsumerProperties, ProducerProperties, OutboxMessageBinderProvisioner>(
        headersToEmbed.toTypedArray(),
        provisioningProvider
    ) {
    companion object{
        @JvmStatic
        private val log = LoggerFactory.getLogger(OutboxMessageBinder::class.java)!!
    }

    public override fun createProducerMessageHandler(
        destination: ProducerDestination,
        producerProperties: ProducerProperties,
        errorChannel: MessageChannel?,
    ): MessageHandler {
        return MessageHandler { message: Message<*> ->
            val destinationTopic = destination
            val payload = String((message.payload as ByteArray))
            val headers = message.headers
            try {
                log.info("received the payload $payload, for destination=${destinationTopic.name}")
                TODO("Step 1 : store message in db")
                TODO("Step 2 : write a scheduler to read and send message to destination")
                TODO("Step 3 : write kafka ACK listner to receive and store the processed messages ")
                TODO("Step 4 : delete the old already sent messages based on retension configuration ")
            } catch (e: IOException) {
                throw RuntimeException(e)
            }
        }
    }

    public override fun createConsumerEndpoint(
        destination: ConsumerDestination,
        group: String,
        properties: ConsumerProperties
    ): MessageProducer {
        throw RuntimeException("Implementation Not required")
    }
}

open class OutboxMessageBinderProvisioner : ProvisioningProvider<ConsumerProperties, ProducerProperties> {
    override fun provisionProducerDestination(
        name: String,
        properties: ProducerProperties
    ): ProducerDestination {
        return MessageDestination(destination = name)
    }

    override fun provisionConsumerDestination(
        name: String,
        group: String,
        properties: ConsumerProperties
    ): ConsumerDestination {
        return MessageDestination(destination = name)
    }
}

private class MessageDestination(private val destination: String) : ProducerDestination,
    ConsumerDestination {
    override fun getName(): String {
        return destination.trim { it <= ' ' }
    }

    override fun getNameForPartition(partition: Int): String {
        throw UnsupportedOperationException("Partitioning is not implemented.")
    }
}