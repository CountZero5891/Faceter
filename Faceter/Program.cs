using System;
using System.IO;
using System.Collections.Generic;
using System.Diagnostics;

namespace Faceter
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Stopwatch stopWatch = new Stopwatch();
            int size_of_chunk = 90000000;//объём буфера
            Console.WriteLine("Идет разбиение файлов...");

            //начало работы
            stopWatch.Start();
            SplitFiles(size_of_chunk);
            MemoryUsage();
            Console.WriteLine("Идет сортировка...");
            Sort();
            MemoryUsage();
            Console.WriteLine("Объединение отсортированных файлов");
            Merge();
            MemoryUsage();
            Console.WriteLine("Объединение завершено");
            stopWatch.Stop();
            Console.WriteLine("Время выполнения");

            //тут уже выводится сколько времени прошло
            TimeSpan ts = stopWatch.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            Console.WriteLine("RunTime " + elapsedTime);
            
            
            //Функцию вывода выключил из-за большого кол-ва записей в файле
            /*Console.WriteLine("Вывод");
            Display();*/
            Console.ReadKey();
        }

        //Функция разделения файлов
        static void SplitFiles(int buffer)
        {
            int split_num = 1;
            StreamWriter sw = new StreamWriter(string.Format("c:\\temp\\split{0:d5}.dat", split_num));
            foreach (string path in Directory.GetFiles("c:\\","BigFile*.txt"))
            {
                Console.WriteLine("{0} \r", path);
                using (StreamReader sr = new StreamReader(path))
                {
                    while (sr.Peek() >= 0)
                    {
                        sw.WriteLine(sr.ReadLine());
                        if (sw.BaseStream.Length > buffer && sr.Peek() >= 0)
                        {
                            sw.Close();
                            split_num++;
                            sw = new StreamWriter(string.Format("c:\\temp\\split{0:d5}.dat", split_num));
                        }
                    }
                    Console.WriteLine("Splitting complete");
                }

            }
            sw.Close();
        }
        //Функция загрузки очередей
        static void LoadQueue(Queue<long> queue, StreamReader file, int records)
        {
            for (int i = 0; i < records; i++)
            {
                if (file.Peek() < 0) 
                    break;
                queue.Enqueue(Int64.Parse(file.ReadLine()));
            }
        }
        //Функция сортировки в буфере
        static void Sort()
        {
            Console.WriteLine("Sorting chunks is starting: ");
            List<long> contents = new List<long>();

            foreach (string path in Directory.GetFiles(@"c:\\temp\\", "split*.dat"))
            {
                Console.WriteLine("{0} \r", path);
                foreach (var line in File.ReadLines(path))
                {
                    contents.Add(Int64.Parse(line));
                }

                contents.Sort();
                string newpath = path.Replace("split", "sorted");
                using (StreamWriter writer = new StreamWriter(newpath))
                {
                    foreach (var s in contents)
                    {
                        writer.WriteLine(s);
                    }
                }

                File.Delete(path);
                contents.Clear();
                GC.Collect();
            }
        }
        //Функция объединения
        static void Merge()
        {
            string[] paths = Directory.GetFiles("c:\\temp\\", "sorted*.dat");
            int chunks = paths.Length; // Кол-во кусков
            int recordsize = 100; // приблизительный размер записи
            int maxusage = 90000000; // макс. памяти
            int buffersize = maxusage / chunks; // размер в байтах каждого буфера
            double recordoverhead = 7.5; // The overhead of using Queue<>
            int bufferlen = (int)(buffersize / recordsize / recordoverhead); // кол-во записей в буфере

            // Открыть файлы
            StreamReader[] readers = new StreamReader[chunks];
            for (int i = 0; i < chunks; i++)
                readers[i] = new StreamReader(paths[i]);

            // Создание очередей
            Queue<long>[] queues = new Queue<long>[chunks];
            for (int i = 0; i < chunks; i++)
                queues[i] = new Queue<long>(bufferlen);

            // Загрузка очередей            
            for (int i = 0; i < chunks; i++)
                LoadQueue(queues[i], readers[i], bufferlen);


            // Объединение файлов
            StreamWriter sw1 = new StreamWriter("c:\\temp\\BigFileSorted3.txt");
            bool done = false;
            int lowest_index, j;
            long lowest_value;
            while (!done)
            {
                // Найти кусок с наим значением
                lowest_index = -1;
                lowest_value = 0;
                for (j = 0; j < chunks; j++)
                {
                    if (queues[j] != null)
                    {
                        if (lowest_index < 0 || queues[j].Peek() < lowest_value)
                        {
                            lowest_index = j;
                            lowest_value = queues[j].Peek();
                        }
                    }
                }

                // если ничего в очереди нет тогда заканчиваем
                if (lowest_index == -1)
                {
                    done = true;
                    break;
                }

                // запись в файл
                sw1.WriteLine(lowest_value);

                // удаление из очереди
                queues[lowest_index].Dequeue();
                
                if (queues[lowest_index].Count == 0)
                {
                    LoadQueue(queues[lowest_index], readers[lowest_index], bufferlen);
                    if (queues[lowest_index].Count == 0)
                    {
                        queues[lowest_index] = null;
                    }
                }
            }
            sw1.Close();

            // Закрываем и удаляем файлы
            for (int i = 0; i < chunks; i++)
            {
                readers[i].Close();
                File.Delete(paths[i]);
            }
        }

        //Сколько памяти было использовано
        static void MemoryUsage()
        {
            Console.WriteLine(String.Format("{0} MB peak working set | {1} MB private bytes",
            Process.GetCurrentProcess().PeakWorkingSet64 / 1024 / 1024,
            Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024
        ));
        }
        //Функция вывода данных из файла 
        static void Display()
        {
            string path = "c:\\temp\\BigFileSorted.txt";
            using (StreamReader sr = new StreamReader(path, System.Text.Encoding.Default))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }
        }
    }
}
