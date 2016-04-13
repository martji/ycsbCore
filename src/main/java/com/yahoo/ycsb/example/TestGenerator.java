package com.yahoo.ycsb.example;

import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new TestGenerator().generateZipfian();
	}

	public void generateHotspot() {
		HotspotIntegerGenerator generator = new HotspotIntegerGenerator(0,100,0.2,0.8);
		int[] counters = new int[101];
		int opcount = 10000;
		for (int i = 0; i < opcount; i++) {
			int index = generator.nextInt();
			counters[index] ++;
		}
		for (int i = 0; i < 101; i++) {
			System.out.println(i + "\t" + counters[i]);
		}
	}

	public void generateZipfian() {
		int nodeSize = 100;
		int itemCount = 10000;
		int[] count = new int[nodeSize];

		int threadNumber = 10;

		MTask task = new MTask(itemCount, nodeSize, count);
		ExecutorService threadPool = Executors.newCachedThreadPool();

		for (int i = 0; i < threadNumber; i++) {
			threadPool.execute(task);
		}

		threadPool.shutdown();

		while (!threadPool.isTerminated()) {
			try {
				Thread.sleep(1*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		for (int i = 0; i < nodeSize; i++) {
			System.out.println(count[i]);
		}
	}

	class MTask implements Runnable {
		private int itemCount;
		private int nodeSize;
		private int[] count;
		private String name = "usertable:user";

		public MTask (int itemCount, int nodeSize, int[] count) {
			this.itemCount = itemCount;
			this.nodeSize = nodeSize;
			this.count = count;
		}

		public void run() {
			CounterGenerator transactioninsertkeysequence = new CounterGenerator(itemCount);
			ZipfianGenerator gen = new ZipfianGenerator(itemCount);
			for (int i = 0; i < 100 * itemCount; i++) {
				int index = 0;
				do {
					String key = name + gen.nextInt();
//					index = Math.abs(key.hashCode() % nodeSize);
					index = Math.abs((int) HashFunction.getInstance().BKDRHash(key) % nodeSize);
				} while (index > transactioninsertkeysequence.lastInt());
				count(index);
			}
		}

		public synchronized void count(int index) {
			int tmp = count[index];
			count[index] = tmp+1;
		}
	}

}
