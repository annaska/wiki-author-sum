
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * Liest aus (kleinen) Wikipedia XML Dump Files (Format wird dabei noch nicht geprüft) die Namen 
 * der Autoren und gibt sie mit der Summe der Edits für Pages in Namespace 0 aus.
 */
public class wikiAuthorEdits {

	static private int NAMESPACE = 0;
	static private String INPUT_DIR = System.getProperty("user.dir")+"/input";
	static private String OUTPUT_DIR = System.getProperty("user.dir")+"/output";
	
	public static void main(String[] args) throws Exception {
	
		//----------------------------------------------------------
		// SAXParser schreibt PageTags als Strings in Collection
		
		File[] inputFiles = new File(INPUT_DIR).listFiles();
		Collection<String> inputStrings = new ArrayList<String>();
		
		if (inputFiles == null) {
			System.err.println("Input-Ordner ist leer/nicht vorhanden."); 
			return;
		} else {
			for (int i = 0; i < inputFiles.length; i++) {
				if (inputFiles[i].isFile() && inputFiles[i].getPath().endsWith(".xml")) {
					inputStrings.addAll(getXMLString(inputFiles[i].getAbsolutePath()));
				}
			}
		}
		
		//----------------------------------------------------------
		// Flink
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple2<String, Integer>> output = env.fromCollection(inputStrings)
			.filter(new FilterFunction<String>(){
				public boolean filter(String arg) throws Exception {
					return (arg.contains("<ns>"+NAMESPACE+"</ns>"));
				}
			}) //  pages mit ns 0
			.flatMap(new PageCounter()) // (page, 1)
			.map(new UsernameTokenizer()) // (username, 1)
			.groupBy(0)								
			.aggregate(Aggregations.SUM, 1); // (username, edits)
		

		File[] outputFiles = new File(OUTPUT_DIR).listFiles();
		if(outputFiles != null){
			for (File file  : outputFiles) {
				if (file.exists()) file.delete(); // Leert Output-Ordner
			}
		}
		
		output.writeAsCsv(OUTPUT_DIR);
		env.execute();
    }
	
	//----------------------------------------------------------
	// Flink-Methoden
	
	public static final class PageCounter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			out.collect(new Tuple2<String, Integer>(value, 1));
		}
	}
	
	public static final class UsernameTokenizer implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
			String[] tokens = value.f0.split("<username>|</username>");
			if(tokens.length==3) {
				return new Tuple2<String, Integer>(tokens[1], value.f1);
			} else return null;
		}
	}

	//----------------------------------------------------------
	// SAX-Parser
	
 	private static Collection<String> getXMLString(String path) { 
		
 		SAXParserFactory factory = SAXParserFactory.newInstance();
 		SAXParser saxParser = null;
 		PageHandler handler = new PageHandler();
 		
 		try {
 			saxParser = factory.newSAXParser();
			saxParser.parse(new File(path), handler);
			return handler.getPageCol();
			
 		} catch (ParserConfigurationException|SAXException|IOException e) {
			System.err.println("Exception beim Ausführen des SAXParsers.");
			e.printStackTrace();
			return null;
		}
 	}
 	
	private static class PageHandler extends DefaultHandler{
		 
		Collection<String> pageCol;
		String pageString = "";
		boolean PAGE = false;
		 
		public Collection<String> getPageCol(){return pageCol;}

		@Override
		public void startDocument() throws SAXException {pageCol=new ArrayList<String>();}

		@Override
		public void endDocument() throws SAXException {}
	 
		@Override
		public void startElement( String namespaceURI, String localName, String qName, Attributes atts){
			if(qName.equals("page")){
				pageString+="<"+qName+">";
				PAGE = true;
			} else if(PAGE) pageString+="<"+qName+">";
		}
		
 		@Override
 		public void endElement( String namespaceURI, String localName, String qName){
 			if(qName.equals("page")){ 
 				PAGE = false;
 				pageCol.add(pageString+="</"+qName+">");
 				pageString="";
 			} else if(PAGE) pageString+="</"+qName+">";
		}

 		@Override
 		public void characters( char[] ch, int start, int length ){
 			if(PAGE) pageString+=String.valueOf(ch).substring(start, start+length);
 		}
	}
}
