package com.ETL.Extraction;

import com.ETL.Interface.DataExtractorAbstrait;
import com.ETL.Interface.DataTransformer;
import com.ETL.Transformation.SalesAmountSummarizer;
import com.ETL.Transformation.UpperCaseTransformer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class Datawarehouse {
    
    public int totalRecordCount;
    public int threadTrigger;
    
    public Datawarehouse(int threadTrigger) {    
        this.threadTrigger = threadTrigger;
        
    }
    
    private static Connection connection(String db , String name , String pass ) throws SQLException {
        String db_url = "jdbc:sqlserver://localhost:1433;" +
        "databaseName="+db+
        ";encrypt=true;trustServerCertificate=true";
        
        Connection con = DriverManager.getConnection(db_url , name , pass ) ;
        return con ;
    }
    
    
    public int checkAndTriggerThread(int totalRecord) {
        this.totalRecordCount = totalRecord;
        int threadCount = (int) Math.ceil( (double)totalRecordCount / threadTrigger);
        return threadCount;
    }
    
   
    public static void load( String db , List<List<String>> result , String table ) throws SQLException{
        Connection con = connection(db , "sa" , "Chaymae_2001") ;
        int ite = 0 ;
        Statement statement=con.createStatement();
        for ( List value : result ){
            System.out.println(value.toString());
            String r = "insert into "+table+" values ("+ value.get(0) +",'"+value.get(1)+"' , '"+ value.get(2) +"' , " +  value.get(3) +");" ;
            System.out.println(r);
            statement.executeUpdate(r);
            ite++ ;
        }
        System.out.println("\n\n"+ite + " nombre d'enregistrement on stockées avec succés dans DataWarehouse");
    }
    

    public static void createThreads (Datawarehouse data, int threadCount, List<DataExtractorAbstrait> extractors,           
        List<Thread> threads, String fileType, String table , List<String> columns , String bd ) throws SQLException {
        
        for (int i = 0; i < threadCount; i++) {
            int startIndex = i * data.threadTrigger;
            int endIndex = startIndex+data.threadTrigger;

            // La dernière partie pourrait avoir moins d'enregistrements
            if (i == threadCount - 1) {
                endIndex = data.totalRecordCount - 1;
            }
            
            //Créer une instance de DataExtractorAbstrait
            DataExtractorAbstrait extractor;

            // Créez votre objet d'extracteur en fonction du type de fichier
            if ("Excel".equals(fileType)) {
                extractor = new ExcelFileExtractor(table, startIndex, endIndex , columns );
            } else if ("CSV".equals(fileType)) {
                extractor = new CSVFileExtractor(table, startIndex, endIndex ,  columns );
            } else {
                extractor = new SQLServerExtractor(table,connection(bd , "sa" , "Chaymae_2001" ), 
                         startIndex, data.threadTrigger , columns );    
            } 

            // Ajouter l'extracteur à la liste
            extractors.add(extractor);

            // Créez le thread et ajoutez-le à la liste
            Thread thread = new Thread((Runnable) extractor);
            threads.add(thread);
        
        }
    }
    
    
    public static void createThreads_Transformation (List<DataTransformer> transformers,           
        List<Thread> threads_transformers ,  List<List<String>> Person ,  List<List<String>> Sales , List<List<String>> result , int upper , int j1 , int j2 ) throws SQLException {
        
        UpperCaseTransformer upperCaseTransformer = new UpperCaseTransformer(upper, Person ) ;
        Thread th1 = new Thread( (Runnable) upperCaseTransformer ) ;
        transformers.add(upperCaseTransformer );
        
        SalesAmountSummarizer salesAmountSummarizer = new SalesAmountSummarizer( result , j1, Person, j2, Sales) ;
        transformers.add(salesAmountSummarizer) ;
        Thread th2 = new Thread( (Runnable) salesAmountSummarizer ) ;
        
        threads_transformers.add(th1);
        threads_transformers.add(th2);
        
    }
}
