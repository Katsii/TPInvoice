package invoice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 * Renvoie le chiffre d'affaire d'un client (somme du montant de ses factures)
	 *
	 * @param id la clé du client à chercher
	 * @return le chiffre d'affaire de ce client ou 0 si pas trouvé
	 * @throws SQLException
	 */
	public float totalForCustomer(int id) throws SQLException {
		String sql = "SELECT SUM(Total) AS Amount FROM Invoice WHERE CustomerID = ?";
		float result = 0;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id); // On fixe le 1° paramètre de la requête
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getFloat("Amount");
				}
			}
		}
		return result;
	}

	/**
	 * Renvoie le nom d'un client à partir de son ID
	 *
	 * @param id la clé du client à chercher
	 * @return le nom du client (LastName) ou null si pas trouvé
	 * @throws SQLException
	 */
	public String nameOfCustomer(int id) throws SQLException {
		String sql = "SELECT LastName FROM Customer WHERE ID = ?";
		String result = null;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getString("LastName");
				}
			}
		}
		return result;
	}

	/**
	 * Transaction permettant de créer une facture pour un client
	 *
	 * @param customer Le client
	 * @param productIDs tableau des numéros de produits à créer dans la facture
	 * @param quantities tableau des quantités de produits à facturer faux sinon Les deux tableaux doivent avoir la même
	 * taille
	 * @throws java.lang.Exception si la transaction a échoué
	 */
	public void createInvoice(CustomerEntity customer, int[] productIDs, int[] quantities) throws Exception {
           
		// On calcule le résultat
		String sqlNewInvoice = "INSERT INTO Invoice (CustomerID) VALUES (?)";
                
                String sqlGetPrice= "SELECT Price FROM Product WHERE ID = ?";
                
                 String sqlNewItem = "INSERT INTO Item (InvoiceID, Item, ProductID, Quantity, Cost) Values(?,?,?,?,?)";
                
                String sqlUpInvoice = "UPDATE INVOICE SET CustomerID=? WHERE ID=?";

                // EXCEPTION
                String mess1 = "Produit Inconnu";
                String mess2 = "Quantite Incorrect";
                String mess3 = "Tableau taille différente";
                
		try (	Connection myConnection = myDataSource.getConnection();
			PreparedStatement smtNewInvoice = myConnection.prepareStatement(sqlNewInvoice);
                        PreparedStatement smtGetPrice = myConnection.prepareStatement(sqlGetPrice);
                        PreparedStatement smtNewItem = myConnection.prepareStatement(sqlNewItem);
                        PreparedStatement smtUpInvoice = myConnection.prepareStatement(sqlUpInvoice);) {
			
// On verifie si le tableau de productsID a la meme taille que le tableau quantite
                    if (productIDs.length==quantities.length){
                        // Si un produit est introuvable dans la table on lance une erreur
                       
                        for (int i = 0; i < productIDs.length; i ++){
                            if (!findProduct(productIDs[i])){
                                throw new Exception(mess1);
                            }   
                        }
                        // Si une quantite de produit est négatif ou equal a zero on lance une erreur
                        for (int i = 0; i < quantities.length; i++){
                            if (quantities[i] <= 0) {
                                throw new Exception(mess2);
                            }   
                        }
                        
                    float priceProduct = 0;
                    int customerID;
                     int invoiceID = 0;
                    //Créer une facture vide associé au client
                        customerID = customer.getCustomerId();
                        smtNewInvoice.setInt(1, customerID);
			smtNewInvoice.executeUpdate();
                        
                        
                        ResultSet clefs = smtNewInvoice.getGeneratedKeys();
                        while(clefs.next()){
                           invoiceID = clefs.getInt(1);
                       }
                        
                        for(int i = 0 ; i < productIDs.length ;i++){
                                smtGetPrice.setInt(1, productIDs[i]);
                                // Un ResultSet pour parcourir les enregistrements du résultat
                                ResultSet rs = smtGetPrice.executeQuery();
                            if (rs.next()) {                                
                                    priceProduct = rs.getFloat("Price");
                            }
                           
                                //Créer une nouvelle ligne de item
                                smtNewItem.setInt(1, invoiceID);
                                smtNewItem.setInt(2,i);
                                smtNewItem.setInt(3,productIDs[i] );
                                smtNewItem.setInt(4, quantities[i]);
                                smtNewItem.setFloat(5, priceProduct);
                                smtNewItem.executeUpdate();
                        }
                        
                        smtUpInvoice.setDouble(1,customerID);
                        smtUpInvoice.setInt(2,invoiceID);
                        smtUpInvoice.executeUpdate();
                    } else {
                       throw new Exception(mess3);
                    }                  
                }catch(Exception ex){
                    throw new Exception(ex.getMessage());
                }    
                    
	}

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws SQLException
	 */
	public int numberOfCustomers() throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Customer";
		try (Connection connection = myDataSource.getConnection();
			Statement stmt = connection.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws SQLException
	 */
	public int numberOfInvoicesForCustomer(int customerId) throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Invoice WHERE CustomerID = ?";

		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerId);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customedID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws SQLException
	 */
	CustomerEntity findCustomer(int customerID) throws SQLException {
		CustomerEntity result = null;

		String sql = "SELECT * FROM Customer WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerID);

			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String name = rs.getString("FirstName");
				String address = rs.getString("Street");
				result = new CustomerEntity(customerID, name, address);
			}
		}
		return result;
	}

          /**
	 * Trouver un produit à partir de sa clé
	 *
	 * @param ID la clé du PRODUCT à rechercher
	 * @return un boolean si on trouve un produit correspondant a la clé
	 * @throws SQLException
	 */
	boolean findProduct(int ID) throws SQLException {
		boolean result = false;

		String sql = "SELECT * FROM Product WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, ID);

			ResultSet rs = stmt.executeQuery();
                        // Si on trouve au moins une ligne correspondant au produit on renvoie vrai
			if (rs.next()) {
				result = true;
			}
		}
		return result;
	}
	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws SQLException
	 */
	List<CustomerEntity> customersInCity(String city) throws SQLException {
		List<CustomerEntity> result = new LinkedList<>();

		String sql = "SELECT * FROM Customer WHERE City = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, city);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					String name = rs.getString("FirstName");
					String address = rs.getString("Street");
					CustomerEntity c = new CustomerEntity(id, name, address);
					result.add(c);
				}
			}
		}

		return result;
	}
        
   
}
