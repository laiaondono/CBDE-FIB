package cbde.labs.hbase_mapreduce.reader;

public class MyHBaseReader_KeyDesign extends MyHBaseReader {

	protected String scanStart() {
		// Indiquem la key des de la qual ens interessa llegir la taula
		return "type_3region_0_0";
	}
	
	protected String scanStop() {
		// Com volem llegir fins al final de la taula, retornem null
		return null;
	}
		
}
