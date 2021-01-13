package rmit;

import java.sql.Connection;
import db.Dataset;
import spatialindex.rtree.RTree;
import spatialindex.spatialindex.Point;
import spatialindex.storagemanager.DiskStorageManager;
import spatialindex.storagemanager.IStorageManager;
import spatialindex.storagemanager.PropertySet;


public class Test {

	public static void main(String[] args)throws Exception
	{
		Dataset ds = new Dataset(Settings.db_user, Settings.db_pass);
		Connection conn = ds.Connect();
		
		String index_file = Settings.rtree_index_location;
		PropertySet ps1 = new PropertySet();
		ps1.setProperty("FileName", index_file + ".rtree");
		IStorageManager diskfile = new DiskStorageManager(ps1);

		PropertySet ps2 = new PropertySet();
		ps2.setProperty("IndexIdentifier", 1);
		RTree tree = new RTree(ps2, diskfile);
		
		IKNN alg = new IKNN(tree, Settings.trip_ids, ds, conn);
		
		String locs = "33.999481201171875,-118.48062133789062,34.15974044799805,-118.50379180908203";
		String []pois = locs.split(",");
		
		int num = pois.length/2;
		double [][]arr = new double[num][2];
		int j = 0;
		for (int i = 0; i < arr.length; i++) {
			arr[i][0] = Double.parseDouble(pois[j]);
			arr[i][1] = Double.parseDouble(pois[j+1]);
			j+=2;
		}
		System.out.println("\nQuery:");
		Point []points = new Point[num];
		for (int i = 0; i < points.length; i++) {
			points[i] = new Point(arr[i]);
			System.out.println("Point " + (i+1) + " -> [" +points[i].toString() +"]");
		}
		System.out.println();
		
		long startTime = System.currentTimeMillis();
		String ids = alg.computeIKNN(points, 10);
		long stopTime = System.currentTimeMillis();
		System.out.println("IKNN query runtime: " + (stopTime - startTime) + " IO: " + alg.iotime);
		System.out.println("\nID\tDISTANCE\n--------------------------\n" + ids);	
	}
	
}
