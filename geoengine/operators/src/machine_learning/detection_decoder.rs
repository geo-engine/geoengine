//! Decoders that turn raw ONNX object-detection model outputs into
//! [`Detection`]s (bounding boxes in input-image pixel space), plus NMS.
//!
//! Raw layouts are channel-major `[C, N]`, i.e. `output[c * N + n]` gives
//! candidate `n`'s value for channel `c`:
//! - YOLO boxes: channels `0..4` are the box, followed by per-class scores.
//!   - `has_objectness = true`  (YOLOv5/v6/v7): channel `4` is objectness,
//!     class scores start at channel `5`.
//!   - `has_objectness = false` (YOLOv8/v9/v10): class scores start at `4`.
//! - The four box channels are either center form (`x_center, y_center, w, h`)
//!   or corner form (`x_min, y_min, x_max, y_max`), all in input-image pixels.

use std::cmp::Ordering;

/// A single object detection in input-image pixel coordinates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Detection {
    pub class_id: u32,
    pub score: f32,
    pub x1: f32,
    pub y1: f32,
    pub x2: f32,
    pub y2: f32,
}

/// How the four box channels are encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoxFormat {
    /// `(x_center, y_center, width, height)`
    Center,
    /// `(x_min, y_min, x_max, y_max)`
    Corners,
}

/// Turns a flat channel-major model output buffer into [`Detection`]s.
pub trait DetectionDecoder {
    fn decode(&self, output: &[f32]) -> Vec<Detection>;
}

/// Decoder for YOLO-style raw (pre-NMS) detect outputs.
pub struct YoloBoxesDecoder {
    pub num_classes: usize,
    pub has_objectness: bool,
    pub box_format: BoxFormat,
}

impl YoloBoxesDecoder {
    pub fn new(num_classes: usize, has_objectness: bool, box_format: BoxFormat) -> Self {
        Self {
            num_classes,
            has_objectness,
            box_format,
        }
    }

    fn num_channels(&self) -> usize {
        4 + usize::from(self.has_objectness) + self.num_classes
    }
}

impl DetectionDecoder for YoloBoxesDecoder {
    fn decode(&self, output: &[f32]) -> Vec<Detection> {
        let num_channels = self.num_channels();
        if output.len() < num_channels {
            return Vec::new();
        }
        let n = output.len() / num_channels;
        let class_offset = 4 + usize::from(self.has_objectness);

        let mut detections = Vec::with_capacity(n.max(1) * self.num_classes.max(1));
        for candidate in 0..n {
            let get = |c: usize| output[c * n + candidate];
            let (mut x1, mut y1, mut x2, mut y2) = match self.box_format {
                BoxFormat::Center => {
                    let xc = get(0);
                    let yc = get(1);
                    let w = get(2);
                    let h = get(3);
                    (xc - w / 2.0, yc - h / 2.0, xc + w / 2.0, yc + h / 2.0)
                }
                BoxFormat::Corners => (get(0), get(1), get(2), get(3)),
            };
            if x1 > x2 {
                std::mem::swap(&mut x1, &mut x2);
            }
            if y1 > y2 {
                std::mem::swap(&mut y1, &mut y2);
            }

            let objectness = if self.has_objectness {
                get(class_offset - 1)
            } else {
                1.0
            };
            let mut best_class = 0u32;
            let mut best_class_score = f32::NEG_INFINITY;
            for (class_idx, c) in (class_offset..class_offset + self.num_classes).enumerate() {
                let score = get(c);
                if score > best_class_score {
                    best_class_score = score;
                    best_class = class_idx as u32;
                }
            }

            if best_class_score.is_finite() {
                let score = objectness * best_class_score;
                detections.push(Detection {
                    class_id: best_class,
                    score,
                    x1,
                    y1,
                    x2,
                    y2,
                });
            }
        }
        detections
    }
}

/// Greedy non-maximum suppression, applied independently per class.
///
/// Returns the indices of the surviving detections, sorted ascending.
pub fn nms(detections: &[Detection], iou_threshold: f32) -> Vec<usize> {
    let mut order: Vec<usize> = (0..detections.len()).collect();
    order.sort_by(|&a, &b| {
        detections[b]
            .score
            .partial_cmp(&detections[a].score)
            .unwrap_or(Ordering::Equal)
    });

    let mut suppressed = vec![false; detections.len()];
    let mut keep = Vec::new();
    for &i in &order {
        if suppressed[i] {
            continue;
        }
        keep.push(i);
        for &j in &order {
            if j == i || suppressed[j] {
                continue;
            }
            if detections[i].class_id == detections[j].class_id
                && bbox_iou(&detections[i], &detections[j]) > iou_threshold
            {
                suppressed[j] = true;
            }
        }
    }
    keep.sort_unstable();
    keep
}

/// Intersection-over-union of two axis-aligned boxes.
pub fn bbox_iou(a: &Detection, b: &Detection) -> f32 {
    let ix1 = a.x1.max(b.x1);
    let iy1 = a.y1.max(b.y1);
    let ix2 = a.x2.min(b.x2);
    let iy2 = a.y2.min(b.y2);
    let inter_w = (ix2 - ix1).max(0.0);
    let inter_h = (iy2 - iy1).max(0.0);
    let inter = inter_w * inter_h;
    let area_a = (a.x2 - a.x1).max(0.0) * (a.y2 - a.y1).max(0.0);
    let area_b = (b.x2 - b.x1).max(0.0) * (b.y2 - b.y1).max(0.0);
    let union = area_a + area_b - inter;
    if union > 0.0 { inter / union } else { 0.0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_center_single_class() {
        let n = 2usize;
        let num_classes = 1;
        let num_channels = 4 + num_classes;
        let mut out = vec![0.0f32; num_channels * n];
        // Channel-major layout: channels are x_center, y_center, width, height, class 0 score.
        out[0] = 100.0;
        out[1] = 110.0;
        out[2] = 100.0;
        out[3] = 100.0;
        out[4] = 50.0;
        out[5] = 50.0;
        out[6] = 50.0;
        out[7] = 50.0;
        out[8] = 0.9;
        out[9] = 0.8;

        let decoder = YoloBoxesDecoder::new(num_classes, false, BoxFormat::Center);
        let dets = decoder.decode(&out);
        assert_eq!(dets.len(), 2);
        assert_eq!(dets[0].class_id, 0);
        assert!((dets[0].x1 - 75.0).abs() < 1e-4);
        assert!((dets[0].y1 - 75.0).abs() < 1e-4);
        assert!((dets[0].x2 - 125.0).abs() < 1e-4);
        assert!((dets[0].y2 - 125.0).abs() < 1e-4);
        assert!((dets[0].score - 0.9).abs() < 1e-4);
        assert!((dets[1].score - 0.8).abs() < 1e-4);
    }

    #[test]
    fn nms_suppresses_overlapping_same_class() {
        let dets = vec![
            Detection {
                class_id: 0,
                score: 0.9,
                x1: 75.0,
                y1: 75.0,
                x2: 125.0,
                y2: 125.0,
            },
            Detection {
                class_id: 0,
                score: 0.8,
                x1: 85.0,
                y1: 75.0,
                x2: 135.0,
                y2: 125.0,
            },
        ];
        assert_eq!(nms(&dets, 0.3), vec![0]);
    }

    #[test]
    fn nms_keeps_different_classes() {
        let dets = vec![
            Detection {
                class_id: 0,
                score: 0.9,
                x1: 75.0,
                y1: 75.0,
                x2: 125.0,
                y2: 125.0,
            },
            Detection {
                class_id: 1,
                score: 0.8,
                x1: 75.0,
                y1: 75.0,
                x2: 125.0,
                y2: 125.0,
            },
        ];
        assert_eq!(nms(&dets, 0.3), vec![0, 1]);
    }

    #[test]
    fn decode_objectness_multiplies_score() {
        let n = 1usize;
        let num_classes = 1;
        let num_channels = 4 + 1 + num_classes;
        let mut out = vec![0.0f32; num_channels * n];
        out[0] = 100.0;
        out[1] = 100.0;
        out[2] = 50.0;
        out[3] = 50.0;
        out[4] = 0.5;
        out[5] = 0.8;

        let decoder = YoloBoxesDecoder::new(num_classes, true, BoxFormat::Center);
        let dets = decoder.decode(&out);
        assert_eq!(dets.len(), 1);
        assert!((dets[0].score - 0.4).abs() < 1e-4);
    }

    #[test]
    fn decode_corners_passthrough() {
        let n = 1usize;
        let num_classes = 1;
        let mut out = vec![0.0f32; 5 * n];
        out[0] = 10.0;
        out[1] = 20.0;
        out[2] = 30.0;
        out[3] = 40.0;
        out[4] = 0.7;

        let decoder = YoloBoxesDecoder::new(num_classes, false, BoxFormat::Corners);
        let dets = decoder.decode(&out);
        assert_eq!(dets.len(), 1);
        assert!((dets[0].x1 - 10.0).abs() < 1e-4);
        assert!((dets[0].y1 - 20.0).abs() < 1e-4);
        assert!((dets[0].x2 - 30.0).abs() < 1e-4);
        assert!((dets[0].y2 - 40.0).abs() < 1e-4);
        assert!((dets[0].score - 0.7).abs() < 1e-4);
    }

    #[test]
    fn iou_disjoint_is_zero() {
        let a = Detection {
            class_id: 0,
            score: 1.0,
            x1: 0.0,
            y1: 0.0,
            x2: 10.0,
            y2: 10.0,
        };
        let b = Detection {
            class_id: 0,
            score: 1.0,
            x1: 20.0,
            y1: 20.0,
            x2: 30.0,
            y2: 30.0,
        };
        assert!(bbox_iou(&a, &b).abs() < 1e-9);
    }

    #[test]
    fn iou_identical_is_one() {
        let a = Detection {
            class_id: 0,
            score: 1.0,
            x1: 0.0,
            y1: 0.0,
            x2: 10.0,
            y2: 10.0,
        };
        assert!((bbox_iou(&a, &a) - 1.0).abs() < 1e-4);
    }
}
