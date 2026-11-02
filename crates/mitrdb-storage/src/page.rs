use crate::crc32c;
use mitrdb_common::{Error, Lsn, MAGIC_BYTES, PAGE_HEADER_SIZE, PAGE_SIZE, PageId, Result};

pub const PAGE_TYPE_RAW: u16 = 0;
pub const PAGE_TYPE_SLOTTED: u16 = 1;
pub const PAGE_TYPE_BTREE_LEAF: u16 = 2;
pub const PAGE_TYPE_BTREE_INTERNAL: u16 = 3;
pub const PAGE_FLAG_FREE: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub magic: u32,
    pub page_type: u16,
    pub flags: u16,
    pub page_id: PageId,
    pub page_lsn: Lsn,
    pub free_space_pointer: u16,
    pub slot_count: u16,
    pub crc32c: u32,
    pub reserved: u32,
}

#[derive(Debug, Clone)]
pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new_empty(page_id: PageId, page_type: u16) -> Self {
        let mut page = Page {
            data: [0u8; PAGE_SIZE],
        };
        page.data[0..4].copy_from_slice(&MAGIC_BYTES.to_le_bytes());
        page.data[4..6].copy_from_slice(&page_type.to_le_bytes());
        page.data[6..8].copy_from_slice(&0u16.to_le_bytes());
        page.data[8..12].copy_from_slice(&page_id.to_le_bytes());
        page.data[12..20].copy_from_slice(&0u64.to_le_bytes());
        let free_space_ptr = PAGE_SIZE as u16;
        page.data[20..22].copy_from_slice(&free_space_ptr.to_le_bytes());
        page.data[22..24].copy_from_slice(&0u16.to_le_bytes());
        page.data[24..28].copy_from_slice(&0u32.to_le_bytes());
        page.data[28..32].copy_from_slice(&0u32.to_le_bytes());
        page.update_checksum();
        page
    }

    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Result<Self> {
        let page = Page { data };
        page.validate()?;
        Ok(page)
    }

    pub fn magic(&self) -> u32 {
        u32::from_le_bytes(self.data[0..4].try_into().unwrap())
    }

    pub fn page_type(&self) -> u16 {
        u16::from_le_bytes(self.data[4..6].try_into().unwrap())
    }

    pub fn set_page_type(&mut self, page_type: u16) {
        self.data[4..6].copy_from_slice(&page_type.to_le_bytes());
    }

    pub fn flags(&self) -> u16 {
        u16::from_le_bytes(self.data[6..8].try_into().unwrap())
    }

    pub fn set_flags(&mut self, flags: u16) {
        self.data[6..8].copy_from_slice(&flags.to_le_bytes());
    }

    pub fn page_id(&self) -> PageId {
        u32::from_le_bytes(self.data[8..12].try_into().unwrap())
    }

    pub fn page_lsn(&self) -> Lsn {
        u64::from_le_bytes(self.data[12..20].try_into().unwrap())
    }

    pub fn set_page_lsn(&mut self, lsn: Lsn) {
        self.data[12..20].copy_from_slice(&lsn.to_le_bytes());
    }

    pub fn free_space_pointer(&self) -> u16 {
        u16::from_le_bytes(self.data[20..22].try_into().unwrap())
    }

    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes(self.data[22..24].try_into().unwrap())
    }

    pub fn crc32c(&self) -> u32 {
        u32::from_le_bytes(self.data[24..28].try_into().unwrap())
    }

    pub fn header(&self) -> PageHeader {
        PageHeader {
            magic: self.magic(),
            page_type: self.page_type(),
            flags: self.flags(),
            page_id: self.page_id(),
            page_lsn: self.page_lsn(),
            free_space_pointer: self.free_space_pointer(),
            slot_count: self.slot_count(),
            crc32c: self.crc32c(),
            reserved: u32::from_le_bytes(self.data[28..32].try_into().unwrap()),
        }
    }

    pub fn compute_checksum(&self) -> u32 {
        let mut buffer = self.data;
        buffer[24..28].copy_from_slice(&0u32.to_le_bytes());
        crc32c::checksum(&buffer)
    }

    pub fn update_checksum(&mut self) {
        let chk = self.compute_checksum();
        self.data[24..28].copy_from_slice(&chk.to_le_bytes());
    }

    pub fn validate_structure(&self) -> Result<()> {
        if self.magic() != MAGIC_BYTES {
            return Err(Error::PageCorrupted("Invalid magic number".to_string()));
        }
        let ptype = self.page_type();
        if ptype != PAGE_TYPE_RAW
            && ptype != PAGE_TYPE_SLOTTED
            && ptype != PAGE_TYPE_BTREE_LEAF
            && ptype != PAGE_TYPE_BTREE_INTERNAL
        {
            return Err(Error::PageCorrupted(format!(
                "Invalid page type: {}",
                ptype
            )));
        }
        let free_space_ptr = self.free_space_pointer() as usize;
        let slot_cnt = self.slot_count() as usize;
        if !(PAGE_HEADER_SIZE..=PAGE_SIZE).contains(&free_space_ptr) {
            return Err(Error::PageCorrupted(
                "Free space pointer exceeds page boundary".to_string(),
            ));
        }
        let slot_end = PAGE_HEADER_SIZE + slot_cnt * 4;
        if slot_end > free_space_ptr {
            return Err(Error::PageCorrupted(
                "Slot array collides with record space".to_string(),
            ));
        }
        for i in 0..slot_cnt {
            let offset_pos = PAGE_HEADER_SIZE + i * 4;
            let offset =
                u16::from_le_bytes(self.data[offset_pos..offset_pos + 2].try_into().unwrap())
                    as usize;
            let len = u16::from_le_bytes(
                self.data[offset_pos + 2..offset_pos + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            if (offset != 0 || len != 0) && (offset < free_space_ptr || offset + len > PAGE_SIZE) {
                return Err(Error::PageCorrupted(format!(
                    "Slot {} record out of bounds: offset {}, len {}",
                    i, offset, len
                )));
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_structure()?;
        let stored_crc = self.crc32c();
        let computed_crc = self.compute_checksum();
        if stored_crc != computed_crc {
            return Err(Error::PageCorrupted(format!(
                "CRC32C mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            )));
        }
        Ok(())
    }

    pub fn insert_record(&mut self, record: &[u8]) -> Result<u16> {
        if self.page_type() != PAGE_TYPE_SLOTTED {
            return Err(Error::PageCorrupted(
                "Cannot insert slotted record into non-slotted page".to_string(),
            ));
        }
        let rec_len = record.len();
        if rec_len > PAGE_SIZE - PAGE_HEADER_SIZE - 4 {
            return Err(Error::OutOfSpace);
        }
        let slot_cnt = self.slot_count() as usize;
        let free_space_ptr = self.free_space_pointer() as usize;
        let slot_end = PAGE_HEADER_SIZE + (slot_cnt + 1) * 4;
        if free_space_ptr < rec_len || free_space_ptr - rec_len < slot_end {
            return Err(Error::OutOfSpace);
        }
        let new_free_ptr = (free_space_ptr - rec_len) as u16;
        let offset = new_free_ptr;
        self.data[offset as usize..offset as usize + rec_len].copy_from_slice(record);
        let slot_offset = PAGE_HEADER_SIZE + slot_cnt * 4;
        self.data[slot_offset..slot_offset + 2].copy_from_slice(&offset.to_le_bytes());
        self.data[slot_offset + 2..slot_offset + 4]
            .copy_from_slice(&(rec_len as u16).to_le_bytes());
        self.data[20..22].copy_from_slice(&new_free_ptr.to_le_bytes());
        self.data[22..24].copy_from_slice(&((slot_cnt + 1) as u16).to_le_bytes());
        Ok(slot_cnt as u16)
    }

    pub fn get_record(&self, slot_id: u16) -> Result<&[u8]> {
        if self.page_type() != PAGE_TYPE_SLOTTED {
            return Err(Error::PageCorrupted(
                "Cannot read slotted record from non-slotted page".to_string(),
            ));
        }
        let slot_cnt = self.slot_count();
        if slot_id >= slot_cnt {
            return Err(Error::SlotOutOfBounds(slot_id as usize));
        }
        let slot_offset = PAGE_HEADER_SIZE + (slot_id as usize) * 4;
        let offset = u16::from_le_bytes(self.data[slot_offset..slot_offset + 2].try_into().unwrap())
            as usize;
        let len = u16::from_le_bytes(
            self.data[slot_offset + 2..slot_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        if offset == 0 && len == 0 {
            return Err(Error::SlotOutOfBounds(slot_id as usize));
        }
        if offset < PAGE_HEADER_SIZE || offset + len > PAGE_SIZE {
            return Err(Error::PageCorrupted("Record length overflow".to_string()));
        }
        Ok(&self.data[offset..offset + len])
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation_and_validation() {
        let page = Page::new_empty(42, PAGE_TYPE_SLOTTED);
        assert_eq!(page.page_id(), 42);
        assert_eq!(page.magic(), MAGIC_BYTES);
        assert_eq!(page.page_lsn(), 0);
        assert!(page.validate().is_ok());
    }

    #[test]
    fn test_page_corruption_detection() {
        let mut page = Page::new_empty(1, PAGE_TYPE_SLOTTED);
        assert!(page.validate().is_ok());
        page.data[100] ^= 0xFF;
        assert!(page.validate().is_err());
    }

    #[test]
    fn test_page_invalid_magic() {
        let mut page = Page::new_empty(1, PAGE_TYPE_SLOTTED);
        page.data[0..4].copy_from_slice(&0x12345678u32.to_le_bytes());
        page.update_checksum();
        let err = page.validate().unwrap_err();
        match err {
            Error::PageCorrupted(msg) => assert!(msg.contains("magic")),
            _ => panic!("Expected PageCorrupted"),
        }
    }

    #[test]
    fn test_record_insertion_and_retrieval() {
        let mut page = Page::new_empty(7, PAGE_TYPE_SLOTTED);
        let rec1 = b"hello mitrdb storage engine";
        let rec2 = b"testing slotted records";
        let slot1 = page.insert_record(rec1).unwrap();
        let slot2 = page.insert_record(rec2).unwrap();
        assert_eq!(slot1, 0);
        assert_eq!(slot2, 1);
        assert_eq!(page.get_record(slot1).unwrap(), rec1);
        assert_eq!(page.get_record(slot2).unwrap(), rec2);
        page.update_checksum();
        assert!(page.validate().is_ok());
    }

    #[test]
    fn test_boundary_overflow_rejection() {
        let mut page = Page::new_empty(9, PAGE_TYPE_SLOTTED);
        let large_payload = vec![0xAA; PAGE_SIZE];
        assert_eq!(page.insert_record(&large_payload), Err(Error::OutOfSpace));
    }
}