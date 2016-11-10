/*

Copyright (c) 2016 Edoardo Pasca

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
package net.edosil.concurrent;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class SortedParallelManager<T,S>{
	
	static AtomicInteger tn = new AtomicInteger(0);
	int threads = 0;
	ArrayList<Integer> randnum;
	private ExecutorService service;
	private boolean isConfigured = false;
	private Queue<T> results = new LinkedList<>();
	private Callable<T> task;
	private Predicate<S> shouldContinue;
	
	public SortedParallelManager(int n, int m){
		threads = n;
		Random rn = new Random();
		randnum = Stream.generate(()-> rn.nextInt(threads))
				.limit(m)
				.collect(ArrayList::new, ArrayList::add , ArrayList::addAll);
		service = Executors.newFixedThreadPool(threads);
		
	}
	public SortedParallelManager(int n){
		
	}
	public SortedParallelManager(){
		
	}
	
	public Future<T>  submit(Callable<T> task) throws Exception {
		if (service == null) throw new Exception("service null"); 
		return service.submit(task);
	}
	
	public void shutdown() {
		if (service != null) service.shutdown();
	}
	public boolean isServiceNull() {
		return service == null;
	}
	public boolean isServiceTerminated() {
		return service.isTerminated();
	}
	
	public SortedParallelManager<T,S> setNThreads(int n) {
		threads = n;
		return this;
	}
	public SortedParallelManager<T,S> setParallelTask(Callable<T> task){
		this.task = task;
		return this;
	}
	public SortedParallelManager<T,S> setIterationCondition(Predicate<S> pred){
		shouldContinue = pred;
		return this;
	}
	
	public SortedParallelManager<T,S> setOnPostExecute(){
		return this;
	} 
	
	public boolean init(){
		if (threads != 0){
		
			Random rn = new Random();
			randnum = Stream.generate(()-> rn.nextInt(threads))
					.limit(threads * 10)
					.collect(ArrayList::new, ArrayList::add , ArrayList::addAll);
			service = Executors.newFixedThreadPool(threads);
			isConfigured = true;
		}
		return isConfigured;
	}
	
	/**
	 * proceeds to launch threads in parallel, retrieves the results sorted
	 * and stores it in the results LinkedList and iterates this again until 
	 * shouldContinue is false. You must pass the iteration condition, update rule 
	 * and starting point
	 * 
	 * @param shouldContinue predicate to indicate the condition 
	 * @param initialCondition
	 * @param updater
	 * @throws Exception
	 */
	public void iterate(Predicate<S> shouldContinue, 
			S initialCondition, 
			UnaryOperator<S> updater) throws Exception{
		
		S currentCondition = initialCondition;
		while(isConfigured && shouldContinue.test(currentCondition)){
			//launch threads();
			ArrayList<Future<T>> futures = new ArrayList<>(threads);
			
			for (int i = 0;i<threads ; i++){
				// submit threads
				futures.add ( 
						submit(() -> task.call() )
						);
				System.out.println("submit thread " + i);
			}
			
			// check termination of the parallel part
			if (!isServiceNull()){
				// wait termination of the tasks
				Boolean end = false;
				
				while (! end ) {
					try{
						Thread.sleep(250);
						end = futures.stream()
								.allMatch(x-> {
									Boolean b = false;
									try{
										b = x.isDone();
									} catch (Exception e){
										e.printStackTrace();
										b = false;
									}
									return b;
								});
					}
					catch (InterruptedException e){
						e.printStackTrace();
					}
				}
			}
			System.out.println("threads have terminated");
			
			
			// (process or) store the results in an sorted manner
			try{

				futures.forEach((x)-> 
					{
						try {
							results.offer(x.get());
						} catch (Exception e){
							e.printStackTrace();
						}
					});
			} catch (Exception e){
				e.printStackTrace();
			}
			
			// update currentCondition
			currentCondition = updater.apply(currentCondition);
			
		}
	}
	
	
public static void main(String[] args) {
		
		int total = 100;
		
		
		// generate 10 thread executor service
		SortedParallelManager<Integer[],Integer> cc = new SortedParallelManager<>();
		boolean ii = cc.setNThreads(10)
		  .setParallelTask(()->cc.call())
		  .init();
		// measure the elapsed time
		Instant start = Instant.now();
		// run multiple times this sequence
		int k = 0;
		try {
				
			cc.iterate(
					x->x<total , 
					k, 
					x-> x + cc.threads);
			
		} catch (Exception e){
			e.printStackTrace();
		}finally {
			cc.shutdown();
		}
		
		// print the whole result
		cc.results.forEach(x->System.out.println(x[0] 	+ " delay " + x[1]));
		Instant stop = Instant.now();
		Integer totaltime = cc.randnum.stream().reduce(0,(x,s)->s + x);
		System.out.print("Elapsed time: " + Duration.between(start, stop));
		System.out.println(" total time " + totaltime);
	}

	
	public static void main2(String[] args) {
		
		int total = 100;
		
		Queue<Integer[]> results = new LinkedList<>();
		
		// generate 10 thread executor service
		SortedParallelManager<Integer[],Integer> cc = new SortedParallelManager<>(10, total);
		// measure the elapsed time
		Instant start = Instant.now();
		// run multiple times this sequence
		int k = 0;
		try {
				
			while (k < total){
			
				ArrayList<Future<Integer[]>> futures = new ArrayList<>(cc.threads);
				
					for (int i = 0;i<cc.threads ; i++){
						// submit 10 threads
						futures.add ( 
								cc.submit(() -> cc.call() )
								);
						System.out.println("submit thread " + i);
					}
				
				
				if (!cc.isServiceNull()){
					// wait termination of the tasks
					Boolean end = false;
					
					while (! end ) {
						try{
							Thread.sleep(250);
							end = futures.stream()
									.allMatch(x-> {
										Boolean b = false;
										try{
											b = x.isDone();
										} catch (Exception e){
											e.printStackTrace();
											b = false;
										}
										return b;
									});
						}
						catch (InterruptedException e){
							e.printStackTrace();
						}
					}
					System.out.println("threads have terminated");
					// (process or) store the results in an sorted manner
					try{
	
						futures.forEach((x)-> 
							{try {
								results.offer(x.get());
							} catch (Exception e){
								e.printStackTrace();
							}
							});
					} catch (Exception e){
						e.printStackTrace();
					}
				}
			k = k + cc.threads;
			}
		} catch (Exception e){
			e.printStackTrace();
		}finally {
			cc.shutdown();
		}
		
		// print the whole result
		results.forEach(x->System.out.println(x[0] 	+ " delay " + x[1]));
		Instant stop = Instant.now();
		Integer totaltime = (Integer) cc.randnum.stream().reduce(0,(x,s)->s + x);
		System.out.print("Elapsed time: " + Duration.between(start, stop));
		System.out.println(" total time " + totaltime);
	}
	
	

	public Integer[] call() {
		// do something
		int i = tn.getAndIncrement(); 
		try {	
			// that is, sleep a random number of seconds between 0 and 10
			Thread.sleep(randnum.get(i) * 1000);
			System.out.println("Executed " + i + " sleep " + randnum.get(i) );
			return new Integer[]{i,randnum.get(i)};
		} catch (InterruptedException e) { 
			e.printStackTrace(); 
		}
		return new Integer[]{-i,randnum.get(i)};
	}
	
	
}