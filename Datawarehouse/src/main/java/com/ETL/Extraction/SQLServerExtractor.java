package com.ETL.Extraction;

import com.ETL.Interface.Affichable;
import com.ETL.Interface.DataExtractorAbstrait;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

class SQLServerExtractor extends DataExtractorAbstrait implements Runnable , Affichable {
    
    private int recordIndex=0 ;
    private final String table ;
    private final Connection jdbcConnection ;
    
    // Constructeur
    public SQLServerExtractor(String table, Connection jdbcConnection , int startIndex , int endIndex ,  
            List<String> colonnesAAfficher ){
        super( startIndex , endIndex ,colonnesAAfficher  );
        this.table = table;
        this.jdbcConnection = jdbcConnection;
    }

   
    @Override
    public void extractData() {
        List<String> result ;
        try {
            String sqlQuery = String.format("SELECT %s FROM %s ORDER BY (SELECT NULL) OFFSET %d "
                        + "ROWS FETCH NEXT %d ROWS ONLY;",String.join(", ", colonnesAAfficher ) , table, 
                        startIndex, endIndex );
            
            //Exécution de la requête et récupération des résultats dans resultSet
            PreparedStatement statement = jdbcConnection.prepareStatement(sqlQuery);
            ResultSet resultSet= statement.executeQuery();
            
            // obtenir le nombre de colonnes dans le résultat d'une requête SQ
            int columnCount = resultSet.getMetaData().getColumnCount();
            
            //parcourir les lignes de ResultSet
            while (resultSet.next()) {
                result = new ArrayList<>() ;
                //récupérer les valeurs de chaque colonne pour chaque ligne.
                for (int i = 1; i <= columnCount; i++) {
                    // Ajouter la valeur de chaque colonne à la liste
                    result.add(resultSet.getString(i));   
                }
                recordIndex++ ;
                data.add(result) ;
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
        } 
    }

    @Override
    public void run() {
        extractData();
        afficherResume() ;
    }

  

    @Override
    public void afficherResume(){
        System.out.println("\n\nLe cas d'etude est SQL server :");
        System.out.println(Thread.currentThread().getName()
                             + ",exécute de la méthode run() à partir de SQL server");
        System.out.println(recordIndex+" d'enregistrement sont lit");
        System.out.println("Lire de l'index : " + startIndex + " à l'index " + (startIndex+endIndex) );   
    }

    
}
