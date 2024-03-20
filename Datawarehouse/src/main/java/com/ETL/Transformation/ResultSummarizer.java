package com.ETL.Transformation;

import com.ETL.Interface.DataTransformer;
import java.util.*;
import java.util.stream.Collectors;

public class ResultSummarizer implements Runnable , DataTransformer {

    private  List<List<String>> result;
    private  List<List<String>>  data ;
    private final int groupIndex;
    private final int sumIndex ;

    public ResultSummarizer(List<List<String>> data, int groupIndex, int sumIndex) {
        this.data = data;
        this.groupIndex = groupIndex;
        this.sumIndex = sumIndex;
    }

    public List<List<String>> getResult() {
        return result;
    }
    
    
    @Override
    public void transformData() {
        // Regroupement des données en fonction de l'index spécifié
        Map<String, List<List<String>>> groupedData = data.stream()
                .collect(Collectors.groupingBy(row -> row.get(groupIndex)));
        
        // Calculer la somme pour chaque groupe
        result = new ArrayList<>();
        groupedData.forEach((key, group) -> {
            double sum = group.stream()
                    .mapToDouble(row -> Double.valueOf(row.get(sumIndex)))
                    .sum();
            
            List<String> summaryRow = new ArrayList<>(group.get(0));
            summaryRow.set(sumIndex, String.valueOf(sum));
            result.add(summaryRow);
        });
        
        for (List<String> sousListe : result ) {
            // Supprimer les éléments en position 3 et 4
            sousListe.remove(3);
            sousListe.remove(3); // Après la première suppression, l'élément en position 4 devient l'élément en position 3
        }
        
    }
    
    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()
                             + ",exécute de la méthode run() à partir de ResultSummarizer ");
        transformData();
    }

}
