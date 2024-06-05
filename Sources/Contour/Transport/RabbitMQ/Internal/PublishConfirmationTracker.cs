using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Contour.Helpers;
using Contour.Sending;

namespace Contour.Transport.RabbitMQ.Internal
{
    /// <summary>
    /// A publish confirmation tracker
    /// </summary>
    internal sealed class PublishConfirmationTracker : IPublishConfirmationTracker
    {
        private readonly ILog logger; 
        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<object>> pending = new ConcurrentDictionary<ulong, TaskCompletionSource<object>>();
        private readonly TimeSpan? confirmationTimeout;
        private readonly Func<TaskCompletionSource<object>, ulong, Task> trackImpl;

        /// <summary>
        /// Initializes a new instance of the <see cref="PublishConfirmationTracker"/> class. 
        /// </summary>
        /// <param name="confirmationTimeout"></param>
        public PublishConfirmationTracker(TimeSpan? confirmationTimeout)
        {
            this.confirmationTimeout = confirmationTimeout;
            this.logger = LogManager.GetLogger($"{this.GetType().FullName}({this.GetHashCode()})");
            this.trackImpl = this.confirmationTimeout.HasValue 
                ? (Func<TaskCompletionSource<object>, ulong, Task>)TrackWithTimeout 
                : TrackInfinite;
        }
        
        public void Dispose()
        {
            while (this.pending.Keys.Count > 0)
            {
                var sequenceNumber = this.pending.Keys.First();
                if (this.pending.TryRemove(sequenceNumber, out var tcs))
                {
                    this.logger.Trace(m => m($"A broker publish confirmation for message with sequence number [{sequenceNumber}] has not been received"));
                    tcs.TrySetException(new UnconfirmedMessageException { SequenceNumber = sequenceNumber });
                }
            }
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
        /// Registers a new message publishing confirmation using next sequence number.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/> which can be used to check if confirmation has been received, the message has been rejected or it cannot be confirmed due to channel failure
        /// </returns>
        public Task Track(ulong nextSequenceNumber)
        {
            var completionSource = new TaskCompletionSource<object>();
            if (!this.pending.TryAdd(nextSequenceNumber, completionSource))
            {
                throw new AlreadyTrackedException { SequenceNumber = nextSequenceNumber };
            }

            return this.trackImpl(completionSource, nextSequenceNumber);
        }

        private Task TrackInfinite(TaskCompletionSource<object> completionSource, ulong nextSequenceNumber)
        {
            return completionSource.Task;
        }

        private async Task TrackWithTimeout(TaskCompletionSource<object> completionSource, ulong nextSequenceNumber)
        {
            using (var cts = new CancellationTokenSource(this.confirmationTimeout.Value))
            {
                var token = cts.Token;
                using (_ = token.Register(() => CancelPending(nextSequenceNumber), useSynchronizationContext: false))
                {
                    await completionSource.Task.ConfigureAwait(false);
                }
            }
        }

        private void CancelPending(ulong nextSequenceNumber)
        {
            this.logger.Trace($"Wait for publish confirmation for message with sequence number [{nextSequenceNumber}] is timed out");
            if (this.pending.TryRemove(nextSequenceNumber, out var completionSource))
            {
                completionSource.TrySetException(new UnconfirmedMessageException { SequenceNumber = nextSequenceNumber });
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
            if (this.pending.TryRemove(sequenceNumber, out var completionSource))
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
}
