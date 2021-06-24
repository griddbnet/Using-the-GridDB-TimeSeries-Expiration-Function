import java.util.Arrays;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import com.toshiba.mwcloud.gs.*;


// Operaton on TimeSeries data
public class TSE {

    static class TSERow {
        @RowKey Date timestamp;
        float value;
    }
	public static void main(String[] args) throws GSException,InterruptedException {

		Properties props = new Properties();
		props.setProperty("notificationAddress", args[0]);
		props.setProperty("notificationPort", args[1]);
		props.setProperty("clusterName", args[2]);
		props.setProperty("user", args[3]);
		props.setProperty("password", args[4]);
		GridStore store = GridStoreFactory.getInstance().getGridStore(props);

        String containerName = "JavaTSE";
        System.out.println("Connected");

        store.dropContainer(containerName);
        ContainerInfo containerInfo = new ContainerInfo();

        List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        columnList.add(new ColumnInfo("timestamp", GSType.TIMESTAMP));
        columnList.add(new ColumnInfo("value", GSType.FLOAT));
        containerInfo.setColumnInfoList(columnList);

        containerInfo.setRowKeyAssigned(true);

        TimeSeriesProperties tsProp = new TimeSeriesProperties();
        tsProp.setRowExpiration(1, TimeUnit.MINUTE);
        tsProp.setExpirationDivisionCount(5);
        containerInfo.setTimeSeriesProperties(tsProp);

        TimeSeries<Row> tse = store.putTimeSeries(containerName, containerInfo, false);

        Random r = new Random();

        for(int i=0; i<20; i++) {
            TimeSeries<TSERow> ts = store.putTimeSeries(containerName ,TSERow.class);
            TSERow row = new TSERow();
            row.timestamp = new Date();
            row.value = r.nextFloat();
            ts.put(row);

		    Query<TSERow> query = ts.query("select * order by timestamp asc limit 1");
            RowSet<TSERow> rs = query.fetch(false);
            if(rs.hasNext()) {
                row = rs.next();
                System.out.println("Oldest row is "+row.timestamp);
            }
            Thread.sleep(10000);

        }
        return ;
    }
}
