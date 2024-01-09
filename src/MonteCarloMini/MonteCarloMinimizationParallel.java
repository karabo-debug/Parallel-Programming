package MonteCarloMini;

import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class MonteCarloMinimizationParallel extends RecursiveTask<Integer>{
	
	static long startTime = 0;
	static long endTime = 0;


	private int low, high; // low and high index of the array
    private SearchParallel[] searches;

	int min=Integer.MAX_VALUE;
    int local_min=Integer.MAX_VALUE;
    static int finder =-1;

	static int SEQUENTIAL_CUTOFF; // threshold to optimize the program


	//timers - note milliseconds
	private static void tick(){
		startTime = System.currentTimeMillis();
	}
	private static void tock(){
		endTime = System.currentTimeMillis(); 
	}

	public MonteCarloMinimizationParallel(int low, int high, SearchParallel[] searches){
        this.low = low;
        this.high = high;
        this.searches = searches;

		int length = searches.length;
        
		// optimizes the parallel execution by using the SEQUENTIAL CUTOFF
		/*if(length <= 30_000){
			SEQUENTIAL_CUTOFF = 9_000;
		}
		else if(length <= 450_000){
			SEQUENTIAL_CUTOFF = 100_000;
		}
		else if(length <= 1_000_000){
			SEQUENTIAL_CUTOFF = 300_000;
		}
		else if(length <= 2_000_000){
			SEQUENTIAL_CUTOFF = 600_000;
		}
		else if(length <= 3_000_000){
			SEQUENTIAL_CUTOFF = 900_000;
		}
		else{
			SEQUENTIAL_CUTOFF = 1_200_000;
		}*/

		/* Use these optimizations for extremely large values for testing on the server(multicore machine)*/
		if(length <= 1_000_000){
			SEQUENTIAL_CUTOFF = 250_000;
		}
		else if(length <= 6_000_000){
			SEQUENTIAL_CUTOFF = 2_500_000;
		}
		else if(length <= 10_000_000){
			SEQUENTIAL_CUTOFF = 3_500_000;
		}
		else if(length <= 30_000_000){
			SEQUENTIAL_CUTOFF = 6_500_000;
		}
		else{
			SEQUENTIAL_CUTOFF = 10_500_000;
		}
    }

	@Override
	protected Integer compute() {
		if (high - low <= SEQUENTIAL_CUTOFF){

            // compute directly
            for(int i = low; i < high; i++){

                local_min = searches[i].find_valleys();
                if((!searches[i].isStopped()) && (local_min < min)){ // don't look at those who stopped because hit existing path
                    min = local_min;
                    finder = i; // keep track of who found it
                }
            }

        } else{

			// find the mid to split the two tasks
            int mid = low + (high - low) / 2;

            // create new objects to split the tasks
            MonteCarloMinimizationParallel left = new MonteCarloMinimizationParallel(low, mid, searches);
            MonteCarloMinimizationParallel right = new MonteCarloMinimizationParallel(mid, high, searches);

            // give the first half to the new threads
            left.fork();

            // do the second half in this thread (run the in the current) thread
            int rightMin = right.compute();

			// wait for the completion of the left subtask (blocks the current until the result of the left subtask is available)
            int leftMin = left.join();
            
			// the minimum value from both subtasks is calculated by comparing the results of the left and right subtasks 
            min = Math.min(leftMin, rightMin);
        }
        return min;
	}
	
    public static void main(String[] args)  {

    	int rows, columns; //grid size
    	double xmin, xmax, ymin, ymax; //x and y terrain limits
    	TerrainArea terrain;  //object to store the heights and grid points visited by searches
    	double searches_density;	// Density - number of Monte Carlo  searches per grid position - usually less than 1!

     	int num_searches;		// Number of searches
    	SearchParallel [] searches;		// Array of searches
    	Random rand = new Random();  //the random number generator
    	
    	if (args.length!=7) {  
    		System.out.println("Incorrect number of command line arguments provided.");   	
    		System.exit(0);
    	}
    	/* Read argument values */
    	rows =Integer.parseInt( args[0] );
    	columns = Integer.parseInt( args[1] );
    	xmin = Double.parseDouble(args[2] );
    	xmax = Double.parseDouble(args[3] );
    	ymin = Double.parseDouble(args[4] );
    	ymax = Double.parseDouble(args[5] );
    	searches_density = Double.parseDouble(args[6] );
    	
    	// Initialize 
    	terrain = new TerrainArea(rows, columns, xmin,xmax,ymin,ymax);
    	num_searches = (int)( rows * columns * searches_density );
    	searches= new SearchParallel [num_searches];
    	for (int i=0;i<num_searches;i++) 
    		searches[i]=new SearchParallel(i+1, rand.nextInt(rows),rand.nextInt(columns),terrain);

		// Initialize Parallel Search
        MonteCarloMinimizationParallel parallel = new MonteCarloMinimizationParallel(0, num_searches, searches);
		
		// Initialize Fork join pool object  
        ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

    	//start timer
    	tick();

		// ForkJoinPool has the pool of worker threads
		int globalmin = forkJoinPool.invoke(parallel);

   		//end timer
   		tock();
   	    
		// Results
		System.out.printf("Run parameters\n");
		System.out.printf("\t Rows: %d, Columns: %d\n", rows, columns);
		System.out.printf("\t x: [%f, %f], y: [%f, %f]\n", xmin, xmax, ymin, ymax );
		System.out.printf("\t Search density: %f (%d searches)\n", searches_density,num_searches );

		/*  Total computation time */
		System.out.printf("Time: %d ms\n",endTime - startTime );
		int tmp=terrain.getGrid_points_visited();
		System.out.printf("Grid points visited: %d  (%2.0f%s)\n",tmp,(tmp/(rows*columns*1.0))*100.0, "%");
		tmp=terrain.getGrid_points_evaluated();
		System.out.printf("Grid points evaluated: %d  (%2.0f%s)\n",tmp,(tmp/(rows*columns*1.0))*100.0, "%");
	
		/* Results*/
		System.out.printf("Global minimum: %d at x=%.1f y=%.1f\n\n", globalmin, terrain.getXcoord(searches[finder].getPos_row()), terrain.getYcoord(searches[finder].getPos_col()) );
    }
}