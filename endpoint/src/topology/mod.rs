use stream::LayerRole;

mod inner;

mod layer;
pub use inner::Inner;
pub use layer::Layer;

pub(crate) trait VisitAddress {
    fn visit<F: FnMut(&str)>(&self, f: F);
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, f: F);
}

impl VisitAddress for Vec<String> {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        for addr in self.iter() {
            f(addr)
        }
    }
    // 每一层可能有多个pool，所以usize表示pool编号，新增LayerRole表示层次
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, mut f: F) {
        for (_i, addr) in self.iter().enumerate() {
            f(LayerRole::Unknow, 0, addr);
        }
    }
}
impl VisitAddress for Vec<(LayerRole, Vec<String>)> {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        for (_role, layers) in self.iter() {
            for addr in layers.iter() {
                f(addr)
            }
        }
    }
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, mut f: F) {
        // for (i, layers) in self.iter().enumerate() {
        for (i, (role, layers)) in self.iter().enumerate() {
            for addr in layers.iter() {
                f(role.clone(), i, addr);
            }
        }
    }
}
