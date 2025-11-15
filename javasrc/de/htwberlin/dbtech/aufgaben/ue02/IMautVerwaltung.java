package de.htwberlin.dbtech.aufgaben.ue02;

import java.sql.Connection;
import java.util.List;


/**
 * Die Schnittstelle enthält die Methoden für eine Mautverwaltung.
 * 
 * @author Patrick Dohmeier
 */
public interface IMautVerwaltung {

	void updateStatusForOnBoardUnit(long fzg_id);

	/**
	 * Liefert den Status eines Fahrzeugerätes zurück.
	 * 
	 * @param fzg_id
	 *            - die ID des Fahrzeuggerätes
	 * @return status - den Status des Fahrzeuggerätes
	 **/
	String getStatusForOnBoardUnit(long fzg_id);

	/**
	 * Liefert die Nutzernummer für eine Mauterhebung, die durch ein Fahrzeug im
	 * Automatischen Verfahren ausgelöst worden ist.
	 * 
	 * @param maut_id
	 *            - die ID aus der Mauterhebung
	 * @return nutzer_id - die Nutzernummer des Fahrzeughalters
	 **/
	int getUsernumber(int maut_id);

	/**
	 * Registriert ein Fahrzeug in der Datenbank für einen bestimmten Nutzer.
	 * 
	 * @param fz_id
	 *            - die eindeutige ID des Fahrzeug
	 * @param sskl_id
	 *            - die ID der Schadstoffklasse mit dem das Fahrzeug angemeldet
	 *            wird
	 * @param nutzer_id
	 *            - der Nutzer auf dem das Fahrzeug angemeldet wird
	 * @param kennzeichen
	 *            - das amtliche Kennzeichen des Fahrzeugs
	 * @param fin
	 *            - die eindeutige Fahrzeugindentifikationsnummer
	 * @param achsen
	 *            - die Anzahl der Achsen, die das Fahrzeug hat
	 * @param gewicht
	 *            - das zulässige Gesamtgewicht des Fahrzeugs
	 * @param zulassungsland
	 *            - die Landesbezeichnung für das Fahrzeug in dem es offiziell
	 *            angemeldet ist
	 * 
	 * **/
	void registerVehicle(long fz_id, int sskl_id, int nutzer_id,
			String kennzeichen, String fin, int achsen, int gewicht,
			String zulassungsland);

	/**
	 * Aktualisiert den Status eines Fahrzeuggerätes in der Datenbank.
	 * 
	 * @param fzg_id
	 *            - die ID des Fahrzeuggerätes
	 * @param status
	 *            - der Status auf dem das Fahrzeuggerät aktualisiert werden
	 *            soll
	 */
	void updateStatusForOnBoardUnit(long fzg_id, String status);

	/**
	 * Löscht ein Fahrzeug in der Datenbank.
	 * 
	 * @param fz_id
	 *            - die eindeutige ID des Fahrzeugs
	 */
	void deleteVehicle(long fz_id);

	/**
	 * liefert eine Liste von Mautabschnitten eines bestimmten Abschnittstypen
	 * zurück. z.B. alle Mautabschnitte der Autobahn A10
	 * 
	 * @param abschnittsTyp
	 *            - der AbschnittsTyp kann bspw. eine bestimmte Autobahn (A10)
	 *            oder Bundesstrasse (B1) sein
	 * @return List<Mautabschnitt> - eine Liste des Abschnittstypen, bspw. alle
	 *         Abschnitte der Autobahn A10
	 **/
	List<Mautabschnitt> getTrackInformations(String abschnittstyp);

	 /**
   * Speichert die uebergebene Datenbankverbindung in einer Instanzvariablen.
   * 
   * @author Ingo Classen
   */
  void setConnection(Connection connection);
}
