﻿using System;
using Contour.Receiving;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Topology;

namespace Contour.Transport.RabbitMQ.Internal
{
    /// <summary>
    /// The rabbit bus defaults.
    /// </summary>
    internal static class RabbitBusDefaults
    {
        /// <summary>
        /// The route resolver builder.
        /// </summary>
        public static Func<IRouteResolverBuilder, IRouteResolver> RouteResolverBuilder = RouteResolverBuilderImpl;

        /// <summary>
        /// The subscription endpoint builder.
        /// </summary>
        public static Func<ISubscriptionEndpointBuilder, ISubscriptionEndpoint> SubscriptionEndpointBuilder = SubscriptionEndpointBuilderImpl;

        public static IProducerSelectorBuilder ProducerSelectorBuilder = new DefaultProducerSelectorBuilder();

        /// <summary>
        /// The route resolver builder impl.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <returns>
        /// The <see cref="IRouteResolver"/>.
        /// </returns>
        private static IRouteResolver RouteResolverBuilderImpl(IRouteResolverBuilder builder)
        {
            string label = builder.Sender.Label.Name;
            ExchangeBuilder exchangeBuilder = Exchange.Named(label).Durable;
            exchangeBuilder = builder.Sender.Options.Delayed
                ? exchangeBuilder.DelayedFanout 
                : exchangeBuilder.Fanout;
            
            Exchange exchange = builder.Topology.Declare(exchangeBuilder);

            return new StaticRouteResolver(exchange);
        }

        /// <summary>
        /// The subscription endpoint builder impl.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <returns>
        /// The <see cref="ISubscriptionEndpoint"/>.
        /// </returns>
        private static ISubscriptionEndpoint SubscriptionEndpointBuilderImpl(ISubscriptionEndpointBuilder builder)
        {
            string label = builder.Receiver.Label.Name;

            string queueName = builder.Endpoint.Address + "." + label;

            var queueBuilder = Queue
                .Named(queueName)
                .Durable;

            var options = builder.Receiver.Options;

            if (options.GetQueueLimit().HasValue)
            {
                queueBuilder.WithLimit(options.GetQueueLimit().Value);
            }

            if (options.GetQueueMaxLengthBytes().HasValue)
            {
                queueBuilder.WithMaxLengthBytes(options.GetQueueMaxLengthBytes().Value);
            }

            Queue queue = builder.Topology.Declare(queueBuilder);
            ExchangeBuilder exchangeBuilder = Exchange.Named(label).Durable;
            exchangeBuilder = builder.Receiver.Options.Delayed
                ? exchangeBuilder.DelayedFanout
                : exchangeBuilder.Fanout;
            
            Exchange exchange = builder.Topology.Declare(exchangeBuilder);

            builder.Topology.Bind(exchange, queue);

            return builder.ListenTo(queue, exchange);
        }
    }
}
