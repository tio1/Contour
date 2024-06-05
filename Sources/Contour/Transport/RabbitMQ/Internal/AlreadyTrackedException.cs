using System;

namespace Contour.Transport.RabbitMQ.Internal
{
    public class AlreadyTrackedException : Exception
    {
        public AlreadyTrackedException() : base("Publish confirmation already tracked for provided sequence number. Possible incorrect usage in multi-threaded environment.")
        {
        }

        /// <summary>
        /// The message sequence number provided by the publisher.
        /// </summary>
        public ulong SequenceNumber { get; set; }
    }
}