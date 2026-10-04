//! 派生小字段沿用原始折叠的去重与seq顺序；持久行保存此状态，重开后旧事件也不能倒退字段。
use std::collections::{BTreeMap, HashSet};
#[derive(Clone, Debug, Default)]
pub(crate) struct SpanEventOrder {
    seen: HashSet<u64>,
    fields: BTreeMap<String, u64>,
}
impl SpanEventOrder {
    pub(crate) fn accept(&mut self, event: u64) -> bool {
        self.seen.insert(event)
    }
    pub(crate) fn field(&mut self, name: &str, seq: u64) -> bool {
        let old = self.fields.entry(name.into()).or_insert(seq);
        if seq < *old {
            return false;
        }
        *old = seq;
        true
    }
    pub(crate) fn heap_bytes(&self) -> usize {
        self.seen.capacity().saturating_mul(16)
            + self
                .fields
                .iter()
                .map(|(key, _)| key.capacity() + 96)
                .sum::<usize>()
    }
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut ids = self.seen.iter().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        out.extend_from_slice(&(ids.len() as u64).to_le_bytes());
        for id in ids {
            out.extend_from_slice(&id.to_le_bytes());
        }
        out.extend_from_slice(&(self.fields.len() as u64).to_le_bytes());
        for (field, seq) in &self.fields {
            out.extend_from_slice(&(field.len() as u64).to_le_bytes());
            out.extend_from_slice(field.as_bytes());
            out.extend_from_slice(&seq.to_le_bytes());
        }
        out
    }
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        fn num(bytes: &[u8], pos: &mut usize) -> Option<u64> {
            let end = pos.checked_add(8)?;
            let n = u64::from_le_bytes(bytes.get(*pos..end)?.try_into().ok()?);
            *pos = end;
            Some(n)
        }
        let mut p = 0;
        let mut value = Self::default();
        let n = usize::try_from(num(bytes, &mut p)?).ok()?;
        if n > bytes.len() / 8 {
            return None;
        }
        for _ in 0..n {
            if !value.seen.insert(num(bytes, &mut p)?) {
                return None;
            }
        }
        let n = usize::try_from(num(bytes, &mut p)?).ok()?;
        if n > bytes.len() / 16 {
            return None;
        }
        for _ in 0..n {
            let len = usize::try_from(num(bytes, &mut p)?).ok()?;
            let end = p.checked_add(len)?;
            let field = std::str::from_utf8(bytes.get(p..end)?).ok()?.to_string();
            p = end;
            let seq = num(bytes, &mut p)?;
            if value.fields.insert(field, seq).is_some() {
                return None;
            }
        }
        (p == bytes.len()).then_some(value)
    }
}
