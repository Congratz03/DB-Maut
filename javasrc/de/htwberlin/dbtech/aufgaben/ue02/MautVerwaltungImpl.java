package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.dbtech.exceptions.DataException;

/**
 * Die Klasse realisiert die Mautverwaltung.
 *
 * @author Patrick Dohmeier
 */
public class MautVerwaltungImpl implements IMautVerwaltung {

	private static final Logger L = LoggerFactory.getLogger(MautVerwaltungImpl.class);
	private Connection connection;

	@Override
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

	@Override
	public void updateStatusForOnBoardUnit(long fzg_id) {

	}

	@Override
	public String getStatusForOnBoardUnit(long fzg_id) {
		String query = "SELECT STATUS FROM FAHRZEUGGERAT WHERE FZG_ID = ?";

		try (PreparedStatement ps = getConnection().prepareStatement(query)) {
			ps.setLong(1, fzg_id);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getString("STATUS");
				}
			}
		} catch (SQLException e) {
			L.error("Fehler beim Abrufen des Status für FZG_ID: " + fzg_id, e);
			throw new DataException("Fehler beim Abrufen des Status.", e);
		}

		L.warn("Kein Status gefunden für FZG_ID: " + fzg_id);
		return "";
	}


	@Override
	public int getUsernumber(int maut_id) {
		// Angepasster Query mit JOINs, um die NUTZER_ID über FAHRZEUGGERAT und FAHRZEUG zu erhalten
		String query = "SELECT F.NUTZER_ID " + // F.NUTZER_ID, da sie aus der FAHRZEUG-Tabelle kommt
				"FROM MAUTERHEBUNG ME " +
				"JOIN FAHRZEUGGERAT FG ON ME.FZG_ID = FG.FZG_ID " +
				"JOIN FAHRZEUG F ON FG.FZ_ID = F.FZ_ID " +
				"WHERE ME.MAUT_ID = ?";

		int nutzer_id = 0; // Standardwert, falls nichts gefunden wird

		try (PreparedStatement ps = getConnection().prepareStatement(query)) {
			ps.setInt(1, maut_id);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					// Auslesen der Spalte NUTZER_ID, die jetzt durch den Alias F referenziert wird
					nutzer_id = rs.getInt("NUTZER_ID"); // Hier muss der Name der Spalte in der FAHRZEUG-Tabelle stehen
				}
			}
		} catch (SQLException e) {
			L.error("Fehler beim Abrufen der Nutzernummer für Maut-ID: " + maut_id, e);
			throw new DataException("Fehler beim Abrufen der Nutzernummer.", e);
		}
		return nutzer_id;
	}
	@Override
	public void registerVehicle(long fz_id, int sskl_id, int nutzer_id, String kennzeichen, String fin, int achsen,
								int gewicht, String zulassungsland) {
		String query = "INSERT INTO FAHRZEUG (FZ_ID, SSKL_ID, NUTZER_ID, KENNZEICHEN, FIN, ACHSEN, GEWICHT, ZULASSUNGSLAND, ANMELDEDATUM) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)"; // Oder NOW(), GETDATE() je nach DB

		try (PreparedStatement ps = getConnection().prepareStatement(query)) {
			ps.setLong(1, fz_id);
			ps.setInt(2, sskl_id);
			ps.setInt(3, nutzer_id);
			ps.setString(4, kennzeichen);
			ps.setString(5, fin);
			ps.setInt(6, achsen);
			ps.setInt(7, gewicht);
			ps.setString(8, zulassungsland);

			int affectedRows = ps.executeUpdate();
			if (affectedRows == 0) {
				L.warn("Fahrzeug mit FZ_ID " + fz_id + " konnte nicht registriert werden.");
				// Optional: Eine spezifischere Exception werfen, wenn das Einfügen fehlschlägt
				throw new DataException("Fehler: Fahrzeug mit FZ_ID " + fz_id + " konnte nicht registriert werden.");
			} else {
				L.info("Fahrzeug mit FZ_ID " + fz_id + " erfolgreich registriert.");
			}

		} catch (SQLException e) {
			L.error("Fehler beim Registrieren des Fahrzeugs mit FZ_ID: " + fz_id, e);
			throw new DataException("Fehler beim Registrieren des Fahrzeugs.", e);
		}
	}

	@Override
	public void updateStatusForOnBoardUnit(long fzg_id, String status) {
		String query = "UPDATE FAHRZEUGGERAT SET STATUS = ? WHERE FZG_ID = ?";

		try (PreparedStatement ps = getConnection().prepareStatement(query)) {
			ps.setString(1, status);
			ps.setLong(2, fzg_id);

			int affectedRows = ps.executeUpdate();
			if (affectedRows == 0) {
				L.warn("Status für Fahrzeuggerät mit FZG_ID " + fzg_id + " wurde nicht aktualisiert. Gerät möglicherweise nicht gefunden.");
				// Optional: Eine spezifischere Exception werfen, wenn kein Eintrag gefunden wurde
				throw new DataException("Fehler: Fahrzeuggerät mit FZG_ID " + fzg_id + " nicht gefunden oder Status bereits aktuell.");
			} else {
				L.info("Status für Fahrzeuggerät mit FZG_ID " + fzg_id + " auf '" + status + "' aktualisiert.");
			}

		} catch (SQLException e) {
			L.error("Fehler beim Aktualisieren des Status für Fahrzeuggerät mit FZG_ID: " + fzg_id, e);
			throw new DataException("Fehler beim Aktualisieren des Status für Fahrzeuggerät.", e);
		}
	}

	@Override
	public void deleteVehicle(long fz_id) {
		String query = "DELETE FROM FAHRZEUG WHERE FZ_ID = ?";

		try (PreparedStatement ps = getConnection().prepareStatement(query)) {
			ps.setLong(1, fz_id);

			int affectedRows = ps.executeUpdate();
			if (affectedRows == 0) {
				L.warn("Fahrzeug mit FZ_ID " + fz_id + " konnte nicht gelöscht werden. Fahrzeug möglicherweise nicht gefunden.");
				// Optional: Eine spezifischere Exception werfen, wenn kein Eintrag gefunden wurde
				throw new DataException("Fehler: Fahrzeug mit FZ_ID " + fz_id + " nicht gefunden oder bereits gelöscht.");
			} else {
				L.info("Fahrzeug mit FZ_ID " + fz_id + " erfolgreich gelöscht.");
			}

		} catch (SQLException e) {
			L.error("Fehler beim Löschen des Fahrzeugs mit FZ_ID: " + fz_id, e);
			throw new DataException("Fehler beim Löschen des Fahrzeugs.", e);
		}
	}

	@Override
	public List<Mautabschnitt> getTrackInformations(String abschnittstyp) {
		List<Mautabschnitt> mautabschnitte = new ArrayList<>();
		// Angepasste Spaltennamen basierend auf der Mautabschnitt-Klasse
		String query = "SELECT abschnitts_id, laenge, start_koordinate, ziel_koordinate, name, abschnittstyp FROM MAUTABSCHNITT WHERE abschnittstyp = ?";

		try (PreparedStatement ps = getConnection().prepareStatement(query)) {
			ps.setString(1, abschnittstyp);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					int abschnitts_id = rs.getInt("abschnitts_id");
					int laenge = rs.getInt("laenge"); // Annahme: laenge ist auch in der DB ein INT
					String start_koordinate = rs.getString("start_koordinate");
					String ziel_koordinate = rs.getString("ziel_koordinate");
					String name = rs.getString("name");
					String typ = rs.getString("abschnittstyp");

					// Verwende den vollständigen Konstruktor der Mautabschnitt-Klasse
					Mautabschnitt mautabschnitt = new Mautabschnitt(
							abschnitts_id,
							laenge,
							start_koordinate,
							ziel_koordinate,
							name,
							typ
					);
					mautabschnitte.add(mautabschnitt);
				}
			}
		} catch (SQLException e) {
			L.error("Fehler beim Abrufen der Mautabschnittsinformationen für Typ: " + abschnittstyp, e);
			throw new DataException("Fehler beim Abrufen der Mautabschnittsinformationen.", e);
		}
		L.info("Anzahl der für Typ '" + abschnittstyp + "' geladenen Mautabschnitte: " + mautabschnitte.size());
		return mautabschnitte;
	}

}