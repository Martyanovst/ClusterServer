using System;

namespace ClusterClient
{
    static class ArrayExtension
    {
        public static string[] Shuffle(this string[] array)
        {
            var random = new Random(Guid.NewGuid().GetHashCode());
            for (var i = array.Length - 1; i >= 1; i--)
            {
                var j = random.Next(i + 1);
                var temp = array[j];
                array[j] = array[i];
                array[i] = temp;
            }
            return array;
        }
    }
}
