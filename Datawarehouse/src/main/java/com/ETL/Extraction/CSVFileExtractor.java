package com.ETL.Extraction;


import com.ETL.Interface.Affichable;
import com.ETL.Interface.DataExtractorAbstrait;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVFileExtractor extends DataExtractorAbstrait implements Runnable , Affichable {

    private final String csvFile;
    private int  recordIndex ;
    
    public CSVFileExtractor(String csvFile, int startIndex, int endIndex , List colonnesAAfficher ) {
        super(startIndex, endIndex , colonnesAAfficher );
        this.csvFile = csvFile;
    }

    @Override
    public void extractData() { 
        List<String> result ;
        
        try (Reader reader = new FileReader(csvFile);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            
            // Vérifiez si toutes les colonnes à afficher existent dans le fichier CSV
            for (String colonne : colonnesAAfficher) {
                if (!csvParser.getHeaderMap().containsKey(colonne)) {
                    System.err.println("La colonne '" + colonne + "' n'existe pas dans le fichier CSV.");
                    System.exit(1);
                }
            }
            
            //Lire le fichier CSV et stocker les resultat dans une liste data
            for (CSVRecord csvRecord : csvParser ) {
                
                //vérifie si recordIndex est compris entre startIndex et endIndex
                
                if ( recordIndex >= startIndex && recordIndex <= endIndex) {
                    result = new ArrayList<>();
                    for (String colonne : colonnesAAfficher) {
                        String valeur = csvRecord.get(colonne);
        
                        result.add(valeur);
                    }
                    data.add(result);
                    
                }
                
                recordIndex++;
                
                
                // Si on a atteint la fin de la portion du fichier, on peut sortir de la boucle
                if (recordIndex > endIndex) {
                    break;
                }
                
            }
            
           
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    @Override
    public void run() {
        extractData();
        afficherResume();
    }

    @Override
    public void afficherResume() {
        System.out.println("\nLe cas d'etude est CSV :");
        System.out.println(Thread.currentThread().getName()
                             + ", ,exécute de la méthode run() à partir de CSV");
        System.out.println(recordIndex+" d'enregistrement sont lit");
        System.out.println("Lire de l'index : " + startIndex + " à l'index " + endIndex );
    }
    
}


