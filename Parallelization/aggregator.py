import pprint
from collections import Counter
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class PartialResult:
    """
    A container for the results computed by a single node on its data chunk.
    This structure is stateless and independent of other nodes.
    """
    total_sum: float = 0.0
    item_count: int = 0
    min_value: float = float('inf')
    max_value: float = float('-inf')
    frequency_map: Dict[Any, int] = None

class Aggregator:
    """
    Receives results from multiple nodes and aggregates them into a final report.
    This class handles the 'Reduce' or 'Gather' step of a parallel workflow.
    """
    def __init__(self):
        """Initializes the state for the final, aggregated results."""
        self.final_sum = 0.0
        self.final_count = 0
        self.final_min = float('inf')
        self.final_max = float('-inf')
        # A Counter is a specialized dict perfect for frequency counts.
        self.final_frequency_map = Counter()

    def aggregate(self, partial_results: list[PartialResult]):
        """
        The main method to process a list of partial results from all nodes.
        
        Args:
            partial_results: A list of PartialResult objects, one from each node.
        """
        print(f"Aggregator received {len(partial_results)} partial results. Starting aggregation...")
        
        for result in partial_results:
            # Aggregate Sum and Count (simple addition)
            self.final_sum += result.total_sum
            self.final_count += result.item_count
            
            # Aggregate Min/Max (find the min of mins and max of maxes)
            if result.min_value < self.final_min:
                self.final_min = result.min_value
            if result.max_value > self.final_max:
                self.final_max = result.max_value
            
            # Aggregate Frequency Map (merge dictionaries by adding counts)
            if result.frequency_map:
                self.final_frequency_map.update(result.frequency_map)

        print("Aggregation complete.")

    def get_final_report(self) -> dict:
        """
        Computes any derived metrics and returns the final, consolidated report.
        
        Returns:
            A dictionary containing the complete, aggregated results.
        """
        # Calculate derived metrics AFTER all aggregation is done.
        # CRITICAL: You cannot average the averages. You must use the final sum and count.
        final_average = (self.final_sum / self.final_count) if self.final_count > 0 else 0
        
        report = {
            "total_records_processed": self.final_count,
            "overall_sum": self.final_sum,
            "overall_average": final_average,
            "overall_minimum": self.final_min if self.final_count > 0 else None,
            "overall_maximum": self.final_max if self.final_count > 0 else None,
            "combined_frequency_map": dict(self.final_frequency_map.most_common(5)) # Show top 5
        }
        return report

if __name__ == "__main__":
    # --- SIMULATE RESULTS FROM 4 PARALLEL NODES ---
    # Each node has processed its own chunk and produced a PartialResult object.

    node1_result = PartialResult(
        total_sum=500, item_count=100, min_value=1, max_value=25, 
        frequency_map={'apple': 10, 'banana': 15}
    )
    
    node2_result = PartialResult(
        total_sum=750, item_count=150, min_value=-5, max_value=30, 
        frequency_map={'apple': 5, 'orange': 20}
    )
    
    node3_result = PartialResult(
        total_sum=300, item_count=50, min_value=2, max_value=15, 
        frequency_map={'banana': 10, 'orange': 10}
    )

    node4_result = PartialResult(
        total_sum=1200, item_count=200, min_value=0, max_value=50,
        frequency_map={'apple': 20, 'grape': 30}
    )

    # This is the list of results that the aggregator function would receive.
    all_partial_results = [node1_result, node2_result, node3_result, node4_result]
    
    # --- RUN THE AGGREGATOR ---
    
    # 1. Create an instance of the Aggregator
    my_aggregator = Aggregator()
    
    # 2. Pass the list of partial results to the aggregate method
    my_aggregator.aggregate(all_partial_results)
    
    # 3. Get the final, consolidated report
    final_report = my_aggregator.get_final_report()
    
    # --- DISPLAY THE FINAL REPORT ---
    print("\n" + "="*40)
    print("      FINAL AGGREGATED REPORT")
    print("="*40)
    pprint.pprint(final_report)