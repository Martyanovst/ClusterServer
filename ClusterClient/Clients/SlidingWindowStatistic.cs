using System;
using System.Collections.Generic;

namespace ClusterClient.Clients
{
    class SlidingWindowStatistic
    {
        private readonly int width;
        private long sum;
        private readonly Queue<long> window = new Queue<long>();
        public SlidingWindowStatistic(int windowWidth)
        {
            width = windowWidth;
            lock (window)
            {
                window.Enqueue(500);
            }
        }

        public long GetValue()
        {
            lock (window)
            {
                return sum / window.Count;
            }
        }

        public void Update(long value)
        {
            lock (window)
            {
                if (window.Count == width)
                    sum -= window.Dequeue();
                window.Enqueue(value);

                sum += value;
            }
        }
    }
}
