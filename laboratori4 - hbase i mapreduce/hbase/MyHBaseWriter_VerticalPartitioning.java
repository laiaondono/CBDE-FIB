package cbde.labs.hbase_mapreduce.writer;

public class MyHBaseWriter_VerticalPartitioning extends MyHBaseWriter {

	//solució 1.1 (create statement): create 'wines2', 'fam1', 'fam2'

	protected String toFamily(String attribute) {
		// Si l'atribut és un "type", "region" o "flav", l'assignem a la "fam1". D'aquesta manera, llegint només una
		// família ja accedim a tots els valors necessaris per a Q1 i no escanegem tots els atributs de la taula.
		if (attribute.equals("type") || attribute.equals("region") || attribute.equals("flav"))
			return "fam1";
		// En cas contrari, afegim l'atribut a la "fam2"
		else return "fam2";
	}
		
}
