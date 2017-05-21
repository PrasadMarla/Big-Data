//########################################
//## Template Code for Big Data Analytics
//## assignment 1, at Stony Brook University
//## Fall 2016

//## <Student Name>
import java.util.*;
import java.util.concurrent.*;

//##########################################################################
//##########################################################################
//# PART I. MapReduce
public abstract class MyMapReduce {
	KVPair[] data;
	int num_map_tasks=4;
	int num_reduce_tasks=3;
	final SharedVariables svs = new SharedVariables(); // This holds shared variables used by multiple threads
	class SharedVariables{
		public volatile List<ReducerTask> map_to_reducer = Collections.synchronizedList(new ArrayList<ReducerTask>());
		/*#stores the reducer task assignment and 
         *#each key-value pair returned from mappers
         *#in the form: [ReducerTask(reduce_task_num, KVPair(k,v)), ...]*/
		public volatile List<KVPair> from_reducer = Collections.synchronizedList(new ArrayList<KVPair>());
		/*#stores key-value pairs returned from reducers
         *#in the form [KVPair(k,v), ...]*/
	}
	
	
	public MyMapReduce(KVPair[] data, int num_map_tasks, int num_reduce_tasks){
		this.data=data; // the "file": list of all key value pairs
		this.num_map_tasks=num_map_tasks; // how many processes to spawn as map tasks
		this.num_reduce_tasks=num_reduce_tasks; // " " " as reduce tasks
	}
	
	// programmer methods (to be overridden by inheriting class)
	public abstract ArrayList<KVPair> map(KVPair kv);
	
	public abstract KVPair reduce(KVPair kv);
	
    //###########################################################
    //#System Code: What the map reduce backend handles
	public void mapTask(KVPair[] data_chunk, List<ReducerTask> map_to_reducer){
		//#runs the mappers and assigns each k,v to a reduce task
		for (KVPair kv : data_chunk){
			//#run mappers:
			ArrayList<KVPair> mapped_kvs = this.map(kv);
			//#assign each kv pair to a reducer task
			for (KVPair mapped_kv:mapped_kvs){
				map_to_reducer.add(new ReducerTask(this.partitionFunction(mapped_kv.k.toString()),mapped_kv));
			}
		}
	}
	
	public int partitionFunction(String k){
		//#given a key returns the reduce task to send it
		int node_number=0;
		//#implement this method
		return node_number;
	}
	
	
	public void reduceTask(ArrayList<KVPair> kvs, List<KVPair> from_reducer){
        //#sort all values for each key into a list 
        //#[TODO]


        //#call reducers on each key paired with a *list* of values
        //#and append the result for each key to from_reducer
        //#[TODO]
	}
	
	
	public List<KVPair> runSystem() throws ExecutionException, InterruptedException{
		/*#runs the full map-reduce system processes on mrObject

        #the following two lists are shared by all processes
        #in order to simulate the communication
        #[DONE]*/
		
		
		/*#divide up the data into chunks accord to num_map_tasks, launch a new process
         *#for each map task, passing the chunk of data to it. 
         *#hint: if chunk contains the data going to a given maptask then the following
         *#      starts a process (Thread or Future class are candidate data types for this variable)
         *#      
         *		if you want to use Future class, you can define variables like this:
         *		manager = Executors.newFixedThreadPool(max_num_of_pools);
		 *		processes = new ArrayList<Future<Runnable>>();
         *          Future process= manager.submit(new Runnable(){
		 *			@Override
		 *			public void run() { //necessary method to be called
		 *			}				
		 *			});
		 *		processes.add(process);
		 *		}
         *#      for loop with p.get()  // this will wait for p to get completed
         *#[TODO]
         *      After using manager, make sure you shutdown : manager.shutdownNow();
         *      else if you want to use Thread
         *      
         *      ArrayList<Thread> processes = new ArrayList<Thread>();
         *      processes.add(new Thread(new Runnable(){
				@Override
				public void run() {					
				}
			}));
			p.start();
			Then, join them with 'p.join();' // wait for all ps to complete tasks
         *      */
		
		System.out.println("map_to_reducer after map tasks complete:");
	
		pprint(svs.map_to_reducer);
		
		/*
		 *#"send" each key-value pair to its assigned reducer by placing each 
         *#into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
		 */
		
		ArrayList<KVPair>[] to_reduce_task= new ArrayList[num_reduce_tasks];
		/*
		 *#[TODO]
         *
         *#launch the reduce tasks as a new process for each. 
         *#[TODO]
         *
         *#join the reduce tasks back
         *#[TODO]
         *
         *#print output from reducer tasks 
         *#[DONE]
		 */
		System.out.println("map_to_reducer after reduce tasks complete:");
		pprint(svs.from_reducer);
		
		/*#return all key-value pairs:
         *#[DONE]
		 * 
		 */
		return svs.from_reducer;
	}
	
	public void pprint(List list){
		if (list.size()==0){
			System.out.println();
			return;
		}
		if (list.get(0) instanceof ReducerTask){
			ReducerTask[] arrayToPrint=Arrays.copyOf(list.toArray(), list.toArray().length, ReducerTask[].class);
			Arrays.sort(arrayToPrint);
			for(ReducerTask elem : arrayToPrint){
				System.out.println(elem.toString());
			}
		}
		
		if (list.get(0) instanceof KVPair){
			KVPair[] arrayToPrint=Arrays.copyOf(list.toArray(), list.toArray().length, KVPair[].class);
			Arrays.sort(arrayToPrint);
			for(KVPair elem : arrayToPrint){
				System.out.println(elem.toString());
			}
			
		}
		
		
	}
}
