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
    internal sealed class PublishConfirmationTracker : IPublishConfirmationTracker
    {
        private readonly ILog logger; 
        private readonly RabbitChannel channel;
        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<object>> pending = new ConcurrentDictionary<ulong, TaskCompletionSource<object>>();

        /// <summary>
        /// Initializes a new instance of the <see cref="PublishConfirmationTracker"/> class. 
        /// </summary>
        /// <param name="channel">
        /// The channel.
        /// </param>
        public PublishConfirmationTracker(RabbitChannel channel)
        {
            this.logger = LogManager.GetLogger($"{this.GetType().FullName}({this.GetHashCode()})");
            this.channel = channel;
            this.channel.Shutdown += this.OnChannelShutdown;
        }
        
        public void Dispose()
        {
            // Do not dispose the channel as its' lifetime is controlled by the producer owning this confirmation tracker

            if (this.channel != null)
            {
                this.channel.Shutdown -= this.OnChannelShutdown;
            }

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
        /// Removes all registered confirmations and rejects all pending messages
        /// </summary>
        public void Reset()
        {
            if (this.pending == null)
            {
                return;
            }

            this.pending.Values.ForEach(v => v.TrySetException(new MessageRejectedException()));
            this.pending.Clear();
        }

        /// <summary>
        /// Registers a new message publishing confirmation using current channel publish sequence number
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/> which can be used to check if confirmation has been received, the message has been rejected or it cannot be confirmed due to channel failure
        /// </returns>
        public Task Track(CancellationToken token)
        {
            if (token.IsCancellationRequested)
                return Task.FromCanceled(token);

            var completionSource = new TaskCompletionSource<object>();
            var nextSeqNo = this.channel.GetNextSeqNo();
            this.pending.AddOrUpdate(nextSeqNo, completionSource, (key, tcs) => new TaskCompletionSource<object>());

            var ret = completionSource.Task;
            if (!token.CanBeCanceled)
                return ret;

            var registration = token.Register(() =>
            {
                if (this.pending.TryRemove(nextSeqNo, out var tcs))
                {
                    tcs.TrySetCanceled(token);
                }
            }, useSynchronizationContext: false);
            ret.ContinueWith(
                _ => registration.Dispose(),
                CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

            return ret;
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
            TaskCompletionSource<object> completionSource;
            if (this.pending.TryGetValue(sequenceNumber, out completionSource))
            {
                TaskCompletionSource<object> tcs;
                
                if (this.pending.TryRemove(sequenceNumber, out tcs))
                {
                    if (confirmed)
                    {
                        completionSource.TrySetResult(null);
                    }
                    else
                    {
                        completionSource.TrySetException(new MessageRejectedException());
                    }
                }
            }
        }

        private void OnChannelShutdown(IChannel sender, ShutdownEventArgs args)
        {
            this.logger.Trace(m => m($"Message confirmation channel in connection [{this.channel.ConnectionId}] has been shut down, abandoning pending publish confirmations"));

            while (this.pending.Keys.Count > 0)
            {
                TaskCompletionSource<object> tcs;
                var sequenceNumber = this.pending.Keys.First();
                if (this.pending.TryRemove(sequenceNumber, out tcs))
                {
                    this.logger.Trace(m => m($"A broker publish confirmation for message with sequence number [{sequenceNumber}] has not been received"));
                    tcs.TrySetException(new UnconfirmedMessageException() { SequenceNumber = sequenceNumber });
                }
            }
        }
    }
}
