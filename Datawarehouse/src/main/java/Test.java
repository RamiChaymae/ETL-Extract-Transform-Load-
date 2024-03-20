
import com.ETL.Extraction.Datawarehouse;
import com.ETL.Interface.DataExtractorAbstrait;
import com.ETL.Interface.DataTransformer;
import com.ETL.Transformation.ResultSummarizer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Test {

   
    public static void main(String[] args) throws SQLException, InterruptedException {
        // Des informations sur les sources SQL
        String Person_sql = "Person.Person" ;
        String Sales_sql = "Sales.SalesOrderHeader" ;
        String BD_sql = "AdventureWorks2014" ;
        
        // Des informations sur les sources CSV
        String Person_csv = "D://employe.csv" ;
        String Sales_csv = "D://sales.csv" ;

        // Des informations sur les sources EXCEL
        String Person_excel = "D://dimEmployee.xlsx" ;
        
        // Liste des insrances des SQLServer , Excel , csv
        List <DataExtractorAbstrait> extractors_Person = new ArrayList<>() ;
        
        List <DataExtractorAbstrait> extractors_Sales = new ArrayList <>();

        // Créez un pool de threads
        List < Thread > threads = new ArrayList < >() ;

        //Instante de Datawarehouse pour loading
        Datawarehouse data = new Datawarehouse (1000) ;
        
        // Le nombre de thread pour chaque File
        System.out.println ( " Le nombre de thread de excel_person est :" + data.checkAndTriggerThread(200) ) ;
        System.out.println ( " Le nombre de thread de csv_person est : "+ data.checkAndTriggerThread(10) ) ;
        System.out.println( " Le nombre de thread de SQL server Person est : " + data.checkAndTriggerThread(5000) ) ;

        System.out.println( " Le nombre de thread de csv_sales est : "+ data.checkAndTriggerThread (10) ) ;
        System.out.println( " Le nombre de thread de SQL server Sales est : " + data.checkAndTriggerThread (5000) ) ;
        
        // Creer les threads pour extraire les donn é es de Person
        Datawarehouse.createThreads ( data , data.checkAndTriggerThread(5000) 
                , extractors_Person , threads , "SQL" , Person_sql , 
                Arrays.asList ( "BusinessEntityID" , "FirstName" , "LastName" ) , BD_sql ) ;
         Datawarehouse . createThreads ( data , data.checkAndTriggerThread(200) , extractors_Person , threads , "Excel" ,
                Person_excel , Arrays.asList ( "EmployeeKey" , "FirstName" , "LastName" ) ,
                null ) ;
        Datawarehouse . createThreads ( data , data.checkAndTriggerThread(10), extractors_Person , threads , "CSV" ,
                Person_csv , Arrays.asList ( "id" , "first_name" , "last_name") , null ) ;

        
        // Creer les threads pour extraire les données de Sales
        Datawarehouse . createThreads ( data , data.checkAndTriggerThread(5000) , extractors_Sales , threads , "SQL" ,
                Sales_sql , Arrays.asList( "SalesOrderID" , "OrderDate" , "CustomerID" , "TaxAmt" ) , BD_sql ) ;
        Datawarehouse . createThreads ( data , data.checkAndTriggerThread(5), extractors_Person , 
                threads , "CSV" , Sales_csv , 
                Arrays.asList ( "SalesOrderID" , "OrderDate" , "CustomerID" , "TaxAmt" ) , null ) ;
    
        
        // Démarrage des threads
        threads.forEach ( thread -> {
                thread . start () ;
        }) ;

        // Attendre la fin de tous les threads
        threads.forEach ( thread -> {
            try {
                thread.join() ;
            } catch ( InterruptedException ex ) {
                Logger.getLogger ( Test.class.getName()).log( Level.SEVERE , null , ex ) ;
            }

        });
        
        // Récupérer les résultats après que tous les threads de excel et csv et sql ont terminé
        List < List < String > > Person = new ArrayList <>() ;
        extractors_Person.forEach ( extractor -> {
            Person.addAll ( extractor.getData () ) ;
        }) ;

        List < List < String > > Sales = new ArrayList <>() ;
        extractors_Sales . forEach ( extractor -> {
            Sales.addAll ( extractor.getData () ) ;
        })  ;
        
     
        
        System.out.println ( " \n \n \n Transformation " ) ;
        // Transformation
        List <DataTransformer> transformers = new ArrayList <>() ;

        // Créez un pool de threads
        List < Thread > threads_transformers = new ArrayList < >() ;

        // List contient les r é sultat des threads
        List < List <String> > result = new ArrayList < >() ;
        
        //Ajouter les threads à la liste
        Datawarehouse.createThreads_Transformation ( transformers,threads_transformers,
                                                    Person , Sales , result , 1 ,0 , 2  ) ;
        
        // Démarrage des threads
        threads_transformers.forEach(thread -> {
            thread.start();
        });

        // Attendre la fin de tous les threads
        threads_transformers.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            }

        });
        
        // Afficher l'en-tête du tableau
        System.out.printf("\n\nLe résultat des 2 threads en parallele");
        System.out.printf("\n\n%-15s %-15s %-15s %-15s %-15s %-15s %n", "ID", "FirstName", "LastName", "OrderID","OrderDate" ,  "TotalAmount" );

        // Afficher les lignes du tableau
        for (int i = 0; i < 5; i++) {
            List<String> ligne = result.get(i);
            System.out.printf("%-15s %-15s %-15s %-15s %-15s %-20s %n", ligne.toArray());
        }
        
         //Aprés avoir le résultat final, on passe à thread 3 pour calculer le somme
        ResultSummarizer resultSummarizer = new ResultSummarizer(result, 0, 5 ) ;
        Thread th3 = new Thread( (Runnable) resultSummarizer ) ;
        th3.start();
        th3.join() ;    
        
        // Afficher l'en-tête du tableau
        System.out.printf("\n\nLe résultat final des 3 threads");
        System.out.printf("\n\n%-15s %-15s %-15s %-15s %n", "ID", "FirstName", "LastName", "TotalSalesAmount");

        // Afficher les lignes du tableau
        for (int i = 0; i < 5; i++) {
            List<String> ligne = resultSummarizer.getResult().get(i);
            System.out.printf("%-15s %-15s %-15s %-15s %n", ligne.toArray());
        }
    
    }
}

        
   