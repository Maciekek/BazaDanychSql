import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import com.mysql.jdbc.ResultSetMetaData;


public class DBConn {

        public static void main(String[] args) throws SQLException {
        Scanner odczyt = new Scanner (System.in);
        Connection con = null;
        Statement st = null;
        String url = "jdbc:mysql://sql.ferus.home.pl/ferus27";
        String user = "ferus27";
        String password = "UgLabs1";
        
        System.out.println("Witaj w programie umozliwiajacym operacje na bazie danych.\n");
        try {
        	con = DriverManager.getConnection(url, user, password);
        	con.setAutoCommit(false);
        	st = con.createStatement();
        	Menu(st,con);
        } catch (Exception e) {
			System.out.println("Nie moge polaczyc z baza ;(");
		}
       
       
 }
    

        
public static void Menu( Statement st,Connection con) throws SQLException, IOException {
       Scanner odczyt = new Scanner (System.in);
        	
        	System.out.println("MENU:\n" +
       		"1- jesli chcesz uzyc polecenia INSERT\n"+
    		"2- jesli chcesz uzyc polecenai SELECT\n" +
    		"3- jesli chcesz uzyc polecenia DELETE\n" +
    		"4- jesli chcesz usunac wybrane rekordy\n"+
    		"5- jesli chcesz uzyc polecenia CREATE TABLE\n" +
    		"6- jesli chcesz uzyc polecenia UPDATE\n" +
    		"7- inne nietypowe zapytania do bazy (wpisywane recznie)\n" +
        	"8- koniec");
            
       		String a=null;
      		a=odczyt.nextLine();

            if (a.equals("1")) {
            	Insert(st,con);
       		}
               
                
       		if (a.equals("2")){
       			Select(st,con);
       		}
    					
            
       		if (a.equals("3")){
       			Delete(st,con);
       		}
                
            
       		if (a.equals("4")){
       			DeleteRecords(st,con);
       		}
       			
       			
       		if (a.equals("5")){
       			CreateTable(st,con);
       			}
       			
       		
       		if (a.equals("6")) {
       			Update(st,con);
       		}
       		if (a.equals("7")) {
       			OtherQuery(st,con);
       		}
       		if (a.equals("8")){
       			System.exit(0);
       		}
       		odczyt.close();
}

public static void Insert(Statement st,Connection con) throws SQLException, IOException {
		Scanner odczyt = new Scanner(System.in);
	
		System.out.println("Wybrales opcje INSERT \n");
		
		do {
			System.out.println("Do jakiej tabeli chchialbys dodac rekrody?");
			String WybranaTabela = odczyt.next();
			
			System.out.println("Dodajesz nowego uzytkownika do tabeli pierwszej: \n" +
					"Wprowadz id: ");
			
			String id = odczyt.next();
			System.out.println("Wprowadz imie: ");
			String imie = odczyt.next();
			System.out.println("Wprowadz nazwisko");
			String nazwisko = odczyt.next();
			st.addBatch("INSERT INTO "+WybranaTabela+" VALUES('"+id+"','"+imie+"', '"+nazwisko+"')");
			int counts[] = st.executeBatch();
			con.commit();
			//System.out.println("INSERT INTO "+WybranaTabela+" VALUES('"+id+"','"+imie+"', '"+nazwisko+"')");
			System.out.println("Przeslano " + counts.length + " INSERT"); 
			
		} while(!odczyt.next().equals("koniec"));
		int counts[] = st.executeBatch();
		
   
    System.out.println("Przes³ano " + counts.length + " Insertow"); 
    odczyt.close();
}

public static void Select(Statement st,Connection con) throws SQLException, IOException {

	Scanner odczyt = new Scanner (System.in);	
	ResultSet result = null;
	
	
	
	System.out.println("Wybrales opcje SELECT. Z jakiej tabeli chcesz wyswietlic dane?");
	String NazwaTabeli = odczyt.next();
	System.out.println("Jakie dane chcesz wysietlic? //podaj nazwe kolumny, jesli wszystkie dane to *");
	String NazwaKolumny = odczyt.next();
	
	result = st.executeQuery("Select "+NazwaKolumny+" from "+ NazwaTabeli);
	ResultSetMetaData rsmd = (ResultSetMetaData) result
			.getMetaData();
	int noColumns = rsmd.getColumnCount();

	for (int i = 0; i < noColumns; i++) {
		System.out.print(rsmd.getColumnName(i + 1) + "\t\t");
	}
	System.out.println();
	
	while (result.next()) {
		for (int i = 0; i < noColumns; i++)
								System.out.print(result.getString(i+1) + "\t\t");
		System.out.println();
	odczyt.close();
	
	}
	
}

public static void Delete(Statement st,Connection con) throws SQLException, IOException{
		Scanner odczyt = new Scanner (System.in);
		System.out.println("Wybrales opcje DELETE." +
				"Wprowadz nazwe tabeli ktora chcesz usunac:");
		String TabelaDel = odczyt.next();
		st.addBatch("DROP Table "+TabelaDel+";");
		
		System.out.println("Czy na pewno chcez usunac tabele "+TabelaDel+"? Zmiany beda nieodwracalne!!! //tab lub nie");
		if (odczyt.next().equals("tak")) {
		System.out.println("Usuwanie tabeli...");
		try {
		st.executeBatch();
		} catch  (Exception e) {
			System.out.println("Niestety nie sie usunac tabeli..."); 
		}
		System.out.println("Tabla "+TabelaDel+" zosta³a usunieta"); 
		}
		odczyt.close();
		
		
}

public static void DeleteRecords(Statement st,Connection con) throws SQLException, IOException {

	Scanner odczyt = new Scanner(System.in);	
	System.out.println("Wybrales opcje wybierania poszczegonych rekordow\n" +
				"Wprowadz nazwe tabeli z ktorej chcesz usunac rekord:");
		String NazwaTabeli = odczyt.next();
		System.out.println("Wprowadz nazwe kolumny z ktorej chcesz usunac rekord");
		String NazwaKolumny = odczyt.next();
		System.out.println("Wprowadz nazwe rekorku ktory chcesz usunac");
		String NazwaRekordu = odczyt.next();
		st.addBatch("DELETE FROM `"+NazwaTabeli+"` WHERE `"+NazwaKolumny+"` = '"+NazwaRekordu+"'");
		
		try {
		st.executeBatch();
		con.commit();
		System.out.println("Rekord z "+NazwaRekordu+" zosta³ usuniêty");
		
		}catch (Exception e) {
			System.out.println("Niestety nie moge wykonac usuniecia rekordu...");
		}
		
		odczyt.close();
		
		
}

public static void CreateTable(Statement st,Connection con) throws SQLException, IOException {
	Scanner odczyt = new Scanner (System.in);
	System.out.println("Wybrales opcje utworzenia nowej tabeli");
		System.out.println("Podaj nazwe tabeli jaka chcesz utworzyc");
		String NazwaTabeli = odczyt.next();
		String[] nazwa = new String[10];
		String[] typ = new String[10];
		
		
		
		
		for (int i=1;i<4;i++){
			System.out.println("Podaj nazwe "+i+" kolumny");
			nazwa[i]=odczyt.next();
			System.out.println("Podaj nazwe "+i+" typu //INT lub VARCHAR(50)");
			typ[i]=odczyt.next();
			
		}
				st.addBatch("CREATE TABLE "+NazwaTabeli+"("+
			         ""+nazwa[1]+" "+typ[1]+"(10),"+
			         ""+nazwa[2]+" "+typ[2]+"(10),"+
			         ""+nazwa[3]+" "+typ[3]+"(10)"+
			         
			       ");");
				System.out.println("CREATE TABLE "+NazwaTabeli+"("+
			         ""+nazwa[1]+" "+typ[1]+"(10),"+
			         ""+nazwa[2]+" "+typ[2]+"(10),"+
			         ""+nazwa[3]+" "+typ[3]+"(10)"+
			         
			       ");");
				try{
				st.executeBatch();
			con.commit();
				System.out.println ("Tabela o nazwie " +NazwaTabeli+ " zostala utworzona");
				} catch (Exception e) {
						System.out.println("Niestety nie udalo sie utworzyc tabeli...");
					}
				
				odczyt.close();
				
				}		
public static void Update (Statement st,Connection con) throws SQLException {

	Scanner odczyt = new Scanner(System.in);	
	System.out.println("Wybrales opcje wybierania poszczegonych rekordow\n" +
				"Wprowadz nazwe tabeli na ktorej chcesz wykonac polecenie UPDATE:");
		String NazwaTabeli = odczyt.next();
		System.out.println("Wprowadz nowa wartosc dla starej kolumny");
		String NazwaKolumny = odczyt.next();
		System.out.println("Wprowadz nazwe kolumny i stara wartosc na ktorej chcesz dokonac zmiany ");
		String Warunek = odczyt.next();
		//st.addBatch("Update `"+NazwaTabeli+"` set "+NazwaKolumny+"  WHERE `"+Warunek+"` ");
		//System.out.println("Update `"+NazwaTabeli+"` set "+NazwaKolumny+"  WHERE `"+Warunek+"` ");
		st.addBatch("Update "+NazwaTabeli+" set "+Warunek+"   WHERE "+NazwaKolumny+" ");
		System.out.println("Update "+NazwaTabeli+" set "+NazwaKolumny+"   WHERE "+Warunek+"");
		try {
		st.executeBatch();
		con.commit();
		System.out.println("Rekord z "+Warunek+" zosta³ zmieniony");
		
		}catch (Exception e) {
			System.out.println("Niestety nie moge wykonac usuniecia rekordu...");
		}
		
		odczyt.close();
		
		
}

public static void OtherQuery (Statement st,Connection con) throws SQLException, IOException {
	DataInputStream in = new DataInputStream(System.in);
	System.out.println("Wprowadz zapytanie zgodnie z formatem sql");
	String query =   in.readLine();
	st.executeUpdate(query);
	//INSERT INTO test VALUES('13','maciej', 'kucharski')
	//update user set id = 1 where id=10; 
	try {
	con.commit();	
	System.out.println("Zapytanie zostalo wyslane");
	} catch (Exception e) {
		System.out.println("Niestety nie udalo sie wykonac zapytania...");
	}
	
	}
	

}	
	








