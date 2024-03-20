package com.ETL.Extraction;

import com.ETL.Interface.Affichable;
import com.ETL.Interface.DataExtractorAbstrait;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;


public class ExcelFileExtractor extends DataExtractorAbstrait implements Runnable , Affichable {
    
    private final String excelFile;
    private int recordIndex = 0;
    
    public ExcelFileExtractor(String excelFile, int startIndex, int endIndex , List colonnesAAfficher ) {
        super(startIndex, endIndex , colonnesAAfficher);
        this.excelFile = excelFile;
    }
    
    
    @Override
    public void extractData() {
        List result ;
        try (FileInputStream fileInputStream = new FileInputStream(excelFile);
                
            Workbook workbook = WorkbookFactory.create(fileInputStream)) {
            
                // Supposons que la feuille de calcul que nous voulons lire est la première
                Sheet sheet = workbook.getSheetAt(0);
                
                // Itérer sur chaque ligne dans le fichier Excel
                for (Row row : sheet) {
                    
                    //Ignorer la premier ligne qui contient les nons des colonnes
                    if (row.getRowNum() == 0 ) {
                        continue;
                    }

                    // Itérer sur chaque cellule dans la ligne                    
                    if (recordIndex >= startIndex && recordIndex <= endIndex) {
                        
                        result = new ArrayList<>();
                            for (Cell cell : row) {
                            // Récupérer le nom de la colonne actuelle
                            String nomColonne = sheet.getRow(0).getCell(cell.getColumnIndex()).getStringCellValue();
                            // Vérifier si la colonne doit être extraite
                            if (colonnesAAfficher.contains(nomColonne)) {
                                result.add(cell.toString());
                            }
                        }
                            
                        data.add(result);
                        recordIndex++ ;
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
        System.out.println("\n\nLe cas d'etude est Excel :");
        System.out.println(Thread.currentThread().getName()
                             + ",exécute de la méthode run() à partir d'Excel");
        System.out.println(recordIndex+" d'enregistrement sont lit");
        System.out.println("Lire de l'index : " + startIndex + " à l'index " + endIndex );
    }
    
}


