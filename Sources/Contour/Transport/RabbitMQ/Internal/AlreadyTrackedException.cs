using System;

namespace Contour.Transport.RabbitMQ.Internal
{
    public class AlreadyTrackedException : Exception
    {
        public AlreadyTrackedException(ulong sequenceNumber)
            : base(
                $"Publish confirmation already tracked for sequence number [{sequenceNumber}]. Possible incorrect usage in multi-threaded environment.")
        {
            SequenceNumber = sequenceNumber;
        }

        /// <summary>
        /// The message sequence number provided by the publisher.
        /// </summary>
        public ulong SequenceNumber { get; set; }
    }
}