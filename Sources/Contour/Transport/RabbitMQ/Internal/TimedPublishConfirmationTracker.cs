using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Contour.Helpers;
using Contour.Sending;
using RabbitMQ.Client;

namespace Contour.Transport.RabbitMQ.Internal
{
    /// <summary>
    /// A publish confirmation tracker
    /// </summary>
    internal sealed class TimedPublishConfirmationTracker : IPublishConfirmationTracker
    {
        private readonly ILog logger; 
        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<object>> pending = new ConcurrentDictionary<ulong, TaskCompletionSource<object>>();
        private readonly TimeSpan confirmationTimeout;

        /// <summary>
        /// Initializes a new instance of the <see cref="PublishConfirmationTracker"/> class. 
        /// </summary>
        /// <param name="confirmationTimeout"></param>
        public TimedPublishConfirmationTracker(TimeSpan confirmationTimeout)
        {
            this.confirmationTimeout = confirmationTimeout;
            this.logger = LogManager.GetLogger($"{this.GetType().FullName}({this.GetHashCode()})");
        }
        
        public void Dispose()
        {
            this.Reset();
        }

        /// <summary>
        /// A handler which can be registered to receive publish confirmations from the broker
        /// </summary>
        /// <param name="confirmed">
        /// Denotes if a message is confirmed by the broker
        /// </param>
        /// <param name="sequenceNumber">
        /// The sequence number of the message being handled
        /// </param>
        /// <param name="multiple">
        /// Denotes if a group of messages with sequence numbers less or equal to the sequence number provided have been confirmed by the broker or not
        /// </param>
        public void HandleConfirmation(bool confirmed, ulong sequenceNumber, bool multiple)
        {
            if (multiple)
            {
                this.pending.Keys
                    .Where(r => r <= sequenceNumber)
                    .ToArray()
                    .ForEach(k => this.ProcessConfirmation(k, confirmed));
            }
            else
            {
                this.ProcessConfirmation(sequenceNumber, confirmed);
            }
        }

        /// <summary>
        /// Registers a new message publishing confirmation using current channel publish sequence number
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/> which can be used to check if confirmation has been received, the message has been rejected or it cannot be confirmed due to channel failure
        /// </returns>
        public async Task Track(ulong nextSequenceNumber)
        {
            using (var cts = new CancellationTokenSource(this.confirmationTimeout))
            {
                var completionSource = new TaskCompletionSource<object>();
                this.pending.AddOrUpdate(nextSequenceNumber, completionSource,
                    (key, tcs) =>
                    {
                        logger.Error($"Already existed TaskCompletionSource for [{key}]");
                        return tcs;
                    });

                logger.Trace(m => m("Start tracking confirmation for [{0}]", nextSequenceNumber));
                var token = cts.Token;
                using (_ = token.Register(() =>
                       {
                           logger.Trace(m => m("Try cancel task for [{0}]", nextSequenceNumber));
                           completionSource.TrySetCanceled(token);
                           this.pending.TryRemove(nextSequenceNumber, out _);
                       }, useSynchronizationContext: false))
                    {
                        await completionSource.Task;
                        logger.Trace(m => m("End tracking confirmation for [{0}]", nextSequenceNumber));
                    }
            }
        }

        /// <summary>
        /// Handles the publish confirmation received from the broker
        /// </summary>
        /// <param name="sequenceNumber">
        /// The message sequence number
        /// </param>
        /// <param name="confirmed">
        /// Denotes if a message with <paramref name="sequenceNumber"/> has been confirmed
        /// </param>
        private void ProcessConfirmation(ulong sequenceNumber, bool confirmed)
        {
            logger.Trace(m => m("Try to end waiting task for [{0}]", sequenceNumber));
            if (this.pending.TryRemove(sequenceNumber, out var completionSource))
            {
                if (confirmed)
                {
                    logger.Trace(m => m("Try to set result for waiting task for [{0}]", sequenceNumber));
                    completionSource.TrySetResult(null);
                }
                else
                {
                    logger.Trace(m => m("Try to throw exception for waiting task for [{0}]", sequenceNumber));
                    completionSource.TrySetException(new MessageRejectedException());
                }
            }
        }

        private void Reset()
        {
            logger.Trace("Start resetting tracker");
            if (this.pending == null)
            {
                return;
            }

            logger.Trace("Start clearing pending");
            this.pending.ForEach(kvp =>
            {
                this.logger.Trace($"A broker publish confirmation for message with sequence number [{kvp.Key}] has not been received");
                kvp.Value.TrySetException(new UnconfirmedMessageException { SequenceNumber = kvp.Key });
            });
            this.pending.Clear();
        }
    }
}
