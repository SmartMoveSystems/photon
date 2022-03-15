package de.komoot.photon;

import de.komoot.photon.nominatim.model.AddressType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

public class PhotonDocTest {

    @Test
    public void testCompleteAddressOverwritesStreet() {
        PhotonDoc doc = simplePhotonDoc();
        
        HashMap<String, String> streetNames = new HashMap<>();
        streetNames.put("name", "parent place street");
        doc.setAddressPartIfNew(AddressType.STREET, streetNames);

        HashMap<String, String> address = new HashMap<>();
        address.put("street", "test street");
        doc.address(address);
        AssertUtil.assertAddressName("test street", doc, AddressType.STREET);
    }

    @Test
    public void testCompleteAddressCreatesStreetIfNonExistantBefore() {
        PhotonDoc doc = simplePhotonDoc();

        HashMap<String, String> address = new HashMap<>();
        address.put("street", "test street");
        doc.address(address);
        AssertUtil.assertAddressName("test street", doc, AddressType.STREET);
    }

    @Test
    public void testAddCountryCode() {
        PhotonDoc doc = new PhotonDoc(1, "W", 2, "highway", "residential").countryCode("de");

        assertNotNull(doc.getCountryCode());
        assertEquals("DE", doc.getCountryCode());
    }

    private PhotonDoc simplePhotonDoc() {
        return new PhotonDoc(1, "W", 2, "highway", "residential").houseNumber("4");
    }

}
