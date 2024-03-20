package com.ETL.Transformation;


import com.ETL.Interface.DataTransformer;
import java.util.List;

public class UpperCaseTransformer implements Runnable , DataTransformer {

    private final int indice ;
    private final List<List<String>> data ;
    
    public UpperCaseTransformer(int indice ,List<List<String>> data ) {
        this.indice = indice;
        this.data = data ;
    }

    public List<List<String>> getData() {
        return data;
    }
    
    @Override
    public void transformData() {
        for ( List liste : data ) {
            String update = (String) liste.get(indice) ;
            liste.set(indice, update.toUpperCase() ) ;
        }
    }
    
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()
                             + ",exécute de la méthode run() à partir de UpperCaseTransformer ");
        transformData() ;
    }
    
}
