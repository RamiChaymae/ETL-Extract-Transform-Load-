package com.ETL.Transformation;


import com.ETL.Interface.DataTransformer;
import java.util.ArrayList;
import java.util.List;

public class SalesAmountSummarizer implements Runnable , DataTransformer {
    
    private final int indice1 ;
    private final int indice2 ;

    private final List<List<String>> Person ;
    private final List<List<String>> Sales ;
    private final List<List<String>> result ;
    
    public SalesAmountSummarizer(List<List<String>> result  ,int indice1, List<List<String>> Person, int indice2, List<List<String>> Sales) {
        this.indice1 = indice1;
        this.indice2 = indice2;
        this.Person = Person;
        this.Sales = Sales;
        this.result = result ;
    }
    
    
    @Override
    public void transformData() {

        for (List<String> row1 : Person) {
            List<String> joinedRow = new ArrayList<>(row1);

            for (List<String> row2 : Sales) {
                // Check if the elements at the specified indices match for joining
                if (row1.size() > indice1 && row2.size() > indice2 && row1.get(indice1).equals(row2.get(indice2))) {
                    // Add elements from the second list to the joined row
                    for (int i = 0; i < row2.size(); i++) {
                        if (i != indice2) {
                            joinedRow.add(row2.get(i));
                        }
                    }
                    result.add(joinedRow);
                    break; // Break after the first match (assuming a one-to-one relationship)
                }
            }

          
        }

    }
    
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()
                             + ",exécute de la méthode run() à partir de SalesAmountSummarizer ");
        transformData();
    }
}
