//! graph.rs —— **真的图式向量 ANN**（替掉 `InMemoryGraphIndex` 的暴力 L2 占位），用来**验证**整个自研
//! 路线里红队点名的最大翻车点：**带过滤的近邻搜索能不能把召回拉回来**。
//!
//! 这不是生产级 HNSW（没分层、没量化、没 SIMD），是一个**可测量**的 NSW（navigable small-world）图：
//! 每个点连最近的 M 个邻居，搜索用带访问集的 beam search。重点全在两种过滤策略的对比：
//!
//! - **post-filter（事后过滤）**：先按向量搜出 ef 个近邻、**再**用谓词筛 —— 谓词选择性一高（命中的点稀疏），
//!   近邻里能活下来的寥寥无几，召回崩。这正是当前 `search_similar` 里 `|_,_| true` 全放行所掩盖的问题。
//! - **in-graph（进图过滤）**：导航时**穿过**不满足谓词的点当路由跳板，只把满足谓词的点收进结果，
//!   停止条件只看"已收够满足谓词的点没有" —— 于是会一直往图深处探到命中的点。这是 ACORN 思路的最小版。
//!
//! 模块自带一个**会失败的测试**：在选择性谓词下实测 in-graph 召回 ≫ post-filter 召回。这就是给红队那条
//! "拉不回 → 3-5 人月变 8-10" 风险的**实证答复**：进图过滤确实把召回救得回来。生产默认路径用
//! 自研磁盘型 [`crate::DiskGraphIndex`]；外部 graph_index 适配层只是可选替换和对标路径。
#![allow(dead_code)]

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Mutex;

use crate::GraphIndex;

/// f32 的全序包装（NaN 也定序），好进二叉堆。
#[derive(Clone, Copy, PartialEq)]
struct OrdF32(f32);
impl Eq for OrdF32 {}
impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for OrdF32 {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&o.0)
    }
}

struct Node {
    tenant_id: Option<u64>,
    active: bool,
    key: (u64, u64),
    vec: Vec<f32>,
    /// 邻居在 nodes 里的下标。
    adj: Vec<usize>,
}

struct GraphState {
    nodes: Vec<Node>,
    /// 每点保留的最大邻居数。
    m: usize,
}

/// 图式向量 ANN（NSW）。`index_embedding` 增点连边，`search` 默认走 in-graph 过滤。
pub struct GraphAnnIndex {
    state: Mutex<GraphState>,
    /// 搜索默认 beam 宽度。
    ef: usize,
}

impl GraphAnnIndex {
    /// `m` = 每点邻居数（建议 8~16）；`ef` = 搜索 beam 宽度（越大越准越慢）。
    pub fn new(m: usize, ef: usize) -> Self {
        Self {
            state: Mutex::new(GraphState {
                nodes: Vec::new(),
                m: m.max(2),
            }),
            ef: ef.max(1),
        }
    }
}

impl Default for GraphAnnIndex {
    fn default() -> Self {
        Self::new(8, 32)
    }
}

fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

impl GraphState {
    /// 插入一点并和已有点里最近的 m 个互相连边（建图 O(n²)，验证够用）。
    fn insert(&mut self, tenant_id: Option<u64>, key: (u64, u64), vec: Vec<f32>) {
        for node in &mut self.nodes {
            if node.tenant_id == tenant_id && node.key == key {
                node.active = false;
            }
        }
        let new_idx = self.nodes.len();
        // 找已有点里最近的 m 个。
        let mut near: Vec<(OrdF32, usize)> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (OrdF32(l2_sq(&vec, &n.vec)), i))
            .collect();
        near.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        near.truncate(self.m);

        self.nodes.push(Node {
            tenant_id,
            active: true,
            key,
            vec,
            adj: near.iter().map(|&(_, i)| i).collect(),
        });

        // 反向连边，并把每个被连点的度数剪到 2*m（保留最近的）。
        let m2 = self.m * 2;
        for &(_, i) in &near {
            self.nodes[i].adj.push(new_idx);
            if self.nodes[i].adj.len() > m2 {
                let base = self.nodes[i].vec.clone();
                let mut neigh = std::mem::take(&mut self.nodes[i].adj);
                neigh.sort_unstable_by(|&a, &b| {
                    OrdF32(l2_sq(&base, &self.nodes[a].vec))
                        .cmp(&OrdF32(l2_sq(&base, &self.nodes[b].vec)))
                });
                neigh.truncate(m2);
                self.nodes[i].adj = neigh;
            }
        }
    }

    /// beam search 核心。`admit` 决定一个点能否进结果集（同时驱动停止条件）；导航**穿过**所有未访问邻居
    /// （含 admit=false 的，当路由跳板）。返回被收进结果的点下标，按距离升序。
    fn beam(&self, query: &[f32], ef: usize, admit: &dyn Fn(usize) -> bool) -> Vec<usize> {
        if self.nodes.is_empty() {
            return Vec::new();
        }
        let mut visited = vec![false; self.nodes.len()];
        // 待扩展前沿：最近的先扩展。
        let mut frontier: BinaryHeap<Reverse<(OrdF32, usize)>> = BinaryHeap::new();
        // 结果集：堆顶是最远的（满了丢最远），只装 admit 通过的点。
        let mut result: BinaryHeap<(OrdF32, usize)> = BinaryHeap::new();

        let start = 0usize; // 入口点
        visited[start] = true;
        let ds = OrdF32(l2_sq(query, &self.nodes[start].vec));
        frontier.push(Reverse((ds, start)));
        if admit(start) {
            result.push((ds, start));
        }

        while let Some(Reverse((cd, cur))) = frontier.pop() {
            // 停止：已收够 ef 个满足谓词的点,且前沿里最近的也比结果里最远的还远 → 收敛。
            if result.len() >= ef {
                if let Some(&(worst, _)) = result.peek() {
                    if cd > worst {
                        break;
                    }
                }
            }
            for &nb in &self.nodes[cur].adj {
                if visited[nb] {
                    continue;
                }
                visited[nb] = true;
                let d = OrdF32(l2_sq(query, &self.nodes[nb].vec));
                frontier.push(Reverse((d, nb))); // 穿过任何点做路由
                if admit(nb) {
                    result.push((d, nb));
                    if result.len() > ef {
                        result.pop(); // 丢最远，保 ef 个最近
                    }
                }
            }
        }

        let mut v: Vec<(OrdF32, usize)> = result.into_vec();
        v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        v.into_iter().map(|(_, i)| i).collect()
    }

    fn key_filter<'a>(
        &'a self,
        filter: &'a dyn Fn(u64, u64) -> bool,
    ) -> impl Fn(usize) -> bool + 'a {
        move |idx: usize| {
            let (t, s) = self.nodes[idx].key;
            self.nodes[idx].active && self.nodes[idx].tenant_id.is_none() && filter(t, s)
        }
    }
}

impl GraphAnnIndex {
    /// 进图过滤搜索（导航穿过不满足谓词的点，只收满足的）。返回 (trace, span, L2 距离)，距离升序、取前 k。
    pub fn search_ingraph(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        let st = self.state.lock().unwrap();
        let pred = st.key_filter(filter);
        let mut out: Vec<(u64, u64, f32)> = st
            .beam(query, ef.max(k), &pred)
            .into_iter()
            .map(|i| {
                (
                    st.nodes[i].key.0,
                    st.nodes[i].key.1,
                    l2_sq(query, &st.nodes[i].vec).sqrt(),
                )
            })
            .collect();
        out.truncate(k);
        out
    }

    /// 事后过滤搜索（先按向量搜 ef 个近邻，再用谓词筛）。选择性谓词下召回会崩 —— 用来做对照。
    pub fn search_postfilter(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        let st = self.state.lock().unwrap();
        let all = st.beam(query, ef.max(k), &|_| true); // 先不带过滤搜近邻
        let mut out: Vec<(u64, u64, f32)> = all
            .into_iter()
            .filter(|&i| {
                let (t, s) = st.nodes[i].key;
                st.nodes[i].active && st.nodes[i].tenant_id.is_none() && filter(t, s)
            })
            .map(|i| {
                (
                    st.nodes[i].key.0,
                    st.nodes[i].key.1,
                    l2_sq(query, &st.nodes[i].vec).sqrt(),
                )
            })
            .collect();
        out.truncate(k);
        out
    }

    /// 暴力精确 top-k（满足谓词的点里），给测试算召回 ground-truth。
    pub fn exact_filtered_topk(
        &self,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64)> {
        let st = self.state.lock().unwrap();
        let mut scored: Vec<(OrdF32, (u64, u64))> = st
            .nodes
            .iter()
            .filter(|n| n.active && n.tenant_id.is_none() && filter(n.key.0, n.key.1))
            .map(|n| (OrdF32(l2_sq(query, &n.vec)), n.key))
            .collect();
        scored.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        scored.truncate(k);
        scored.into_iter().map(|(_, key)| key).collect()
    }
}

impl GraphIndex for GraphAnnIndex {
    fn supports_tenant_scope(&self) -> bool {
        true
    }
    fn index_embedding(&self, trace_id: u64, span_id: u64, embedding: Vec<f32>) {
        self.state
            .lock()
            .unwrap()
            .insert(None, (trace_id, span_id), embedding);
    }

    fn index_embedding_scoped(
        &self,
        tenant_id: Option<u64>,
        trace_id: u64,
        span_id: u64,
        embedding: Vec<f32>,
    ) -> std::io::Result<()> {
        self.state
            .lock()
            .unwrap()
            .insert(tenant_id, (trace_id, span_id), embedding);
        Ok(())
    }
    fn search_scoped(
        &self,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(Option<u64>, u64, u64) -> bool,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        let state = self.state.lock().unwrap();
        let admit = |i: usize| {
            let n = &state.nodes[i];
            n.active && filter(n.tenant_id, n.key.0, n.key.1)
        };
        state
            .beam(query, self.ef.max(k), &admit)
            .into_iter()
            .take(k)
            .map(|i| {
                let n = &state.nodes[i];
                (n.tenant_id, n.key.0, n.key.1, l2_sq(query, &n.vec).sqrt())
            })
            .collect()
    }
    fn clear(&self) {
        self.state.lock().unwrap().nodes.clear();
    }

    /// 引擎默认走 **in-graph** 过滤（好的那条）。
    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        self.search_ingraph(query, k, self.ef, filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 确定性伪随机（LCG），不依赖 rand、可复算。
    struct Lcg(u64);
    impl Lcg {
        fn next_f32(&mut self) -> f32 {
            // 经典 LCG 常数
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((self.0 >> 33) as f32) / (1u64 << 31) as f32
        }
        fn vec(&mut self, dim: usize) -> Vec<f32> {
            (0..dim).map(|_| self.next_f32()).collect()
        }
    }

    fn recall(got: &[(u64, u64, f32)], truth: &[(u64, u64)]) -> f32 {
        if truth.is_empty() {
            return 1.0;
        }
        let hit = got
            .iter()
            .filter(|(t, s, _)| truth.contains(&(*t, *s)))
            .count();
        hit as f32 / truth.len() as f32
    }

    /// 一组选择性下的召回测量结果（多查询点平均 + 最差）。
    struct RecallStat {
        selectivity: f64,
        match_count: usize,
        post_mean: f32,
        in_mean: f32,
        in_worst: f32,
    }

    /// 建一张 n 点的图,约 1/`one_in` 的点打 label（trace_id==1）；在多个命中点上各跑一次召回,
    /// 返回 post-filter / in-graph 的均值与 in-graph 最差值。`seed` 让每组选择性用不同随机流。
    fn measure_recall(n: u64, one_in: u64, seed: u64) -> RecallStat {
        let idx = GraphAnnIndex::new(12, 48);
        let mut rng = Lcg(seed);
        let dim = 12;
        let mut matching: Vec<(u64, u64)> = Vec::new();
        for i in 0..n {
            let v = rng.vec(dim);
            let is_match = i % one_in == 0;
            let trace_id = if is_match { 1 } else { 0 };
            idx.index_embedding(trace_id, i, v);
            if is_match {
                matching.push((trace_id, i));
            }
        }
        let filter = |t: u64, _s: u64| t == 1;
        let k = 10;

        // 多个查询点：沿命中集均匀取 8 个（不同密度的邻域），各自的向量当查询。避免"挑一个好点"。
        let probes = 8usize.min(matching.len());
        let mut post_sum = 0.0f32;
        let mut in_sum = 0.0f32;
        let mut in_worst = 1.0f32;
        for p in 0..probes {
            let probe_key = matching[(matching.len() - 1) * p / probes.max(1)];
            let probe_vec = {
                let st = idx.state.lock().unwrap();
                st.nodes
                    .iter()
                    .find(|node| node.key == probe_key)
                    .unwrap()
                    .vec
                    .clone()
            };
            let truth = idx.exact_filtered_topk(&probe_vec, k, &filter);
            let r_post = recall(&idx.search_postfilter(&probe_vec, k, 48, &filter), &truth);
            let r_in = recall(&idx.search_ingraph(&probe_vec, k, 48, &filter), &truth);
            post_sum += r_post;
            in_sum += r_in;
            in_worst = in_worst.min(r_in);
        }
        let d = probes.max(1) as f32;
        RecallStat {
            selectivity: 1.0 / one_in as f64,
            match_count: matching.len(),
            post_mean: post_sum / d,
            in_mean: in_sum / d,
            in_worst,
        }
    }

    #[test]
    fn in_graph_filter_recovers_recall_across_selectivities() {
        // 红队最大翻车点的实证,**表驱动**:多组选择性 × 每组多查询点平均,报均值+最差,不靠单点。
        // 结论要稳:每组里 in-graph 召回 ≥ post-filter,且越稀疏（谓词选择性越高）post-filter 崩得越狠。
        // one_in: 100→1%、20→5%、10→10%、5→20%。每组换一个随机种子。
        let cases = [(100u64, 0xA11Cu64), (20, 0xB22D), (10, 0xC33E), (5, 0xD44F)];
        let n = 800u64;
        let mut stats: Vec<RecallStat> = Vec::new();
        for (one_in, seed) in cases {
            let s = measure_recall(n, one_in, seed);
            eprintln!(
                "[带过滤ANN召回] 选择性≈{:.0}%  命中集={:>3}  post-filter 均值={:.2}  in-graph 均值={:.2} 最差={:.2}",
                s.selectivity * 100.0,
                s.match_count,
                s.post_mean,
                s.in_mean,
                s.in_worst
            );
            // 每组：in-graph 均值不低于 post-filter（进图过滤至少不输事后过滤）。
            assert!(
                s.in_mean >= s.post_mean,
                "选择性 {:.0}%: in-graph 均值({:.2}) 应 ≥ post-filter 均值({:.2})",
                s.selectivity * 100.0,
                s.in_mean,
                s.post_mean
            );
            // 每组：in-graph 均值够用（留余量,不卡死单点）。
            assert!(
                s.in_mean >= 0.75,
                "选择性 {:.0}%: in-graph 均值应 ≥0.75,实测 {:.2}",
                s.selectivity * 100.0,
                s.in_mean
            );
            stats.push(s);
        }
        // 招牌结论的可复现版:在最稀疏那组（1%）,post-filter 明显崩,in-graph 明显高。
        let sparsest = &stats[0];
        assert!(
            sparsest.in_mean - sparsest.post_mean >= 0.2,
            "最稀疏组 in-graph 应显著高于 post-filter,差值实测 {:.2}",
            sparsest.in_mean - sparsest.post_mean
        );
    }

    #[test]
    fn in_graph_results_satisfy_predicate_and_sorted() {
        // 进图过滤的结果都满足谓词、且按距离升序（结构正确性,与召回数值无关）。
        let s = GraphAnnIndex::new(12, 48);
        let mut rng = Lcg(0x1234_5678);
        let mut matching = 0;
        for i in 0..400u64 {
            let is_match = i % 10 == 0;
            s.index_embedding(if is_match { 1 } else { 0 }, i, rng.vec(12));
            matching += is_match as i32;
        }
        assert!(matching > 0);
        let probe = { s.state.lock().unwrap().nodes[10].vec.clone() };
        let ingraph = s.search_ingraph(&probe, 10, 48, &|t, _| t == 1);
        assert!(ingraph.iter().all(|&(t, _, _)| t == 1), "结果都满足谓词");
        assert!(ingraph.windows(2).all(|w| w[0].2 <= w[1].2), "按距离升序");
    }

    #[test]
    fn finds_near_neighbor_without_filter() {
        // 无过滤时,图搜索能找到最近点（基本正确性）。
        let idx = GraphAnnIndex::new(8, 32);
        let mut rng = Lcg(99);
        for i in 0..200u64 {
            idx.index_embedding(0, i, rng.vec(8));
        }
        // 插一个已知点,查它自己 → 应排第一（距离 0）。
        let target = vec![0.5f32; 8];
        idx.index_embedding(7, 9999, target.clone());
        let hits = idx.search(&target, 5, &|_, _| true);
        assert_eq!((hits[0].0, hits[0].1), (7, 9999), "查询点自身应排第一");
        assert!(hits[0].2 < 1e-6, "自身距离 ~0");
    }
}
