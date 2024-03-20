package com.ETL.Interface;



import java.util.ArrayList;
import java.util.List;


public abstract class DataExtractorAbstrait implements DataExtractor  {
    
    protected List<List> data = new ArrayList<>();
    protected final  int startIndex;
    protected final  int endIndex;
    protected final List<String> colonnesAAfficher ;
    
    // Constructeur
    public DataExtractorAbstrait(int startIndex, int endIndex ,List<String> colonnesAAfficher  ) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.colonnesAAfficher = colonnesAAfficher ;
    }
    
    // Méthode pour récupérer le résultat
    public List getData() {
        return this.data;
    }
    
    @Override
    public abstract void extractData();
    
}


