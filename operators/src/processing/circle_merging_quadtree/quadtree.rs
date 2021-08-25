use std::collections::VecDeque;

use geoengine_datatypes::primitives::{BoundingBox2D, Circle, Coordinate2D};

use super::{circle_of_points::CircleOfPoints, circle_radius_model::CircleRadiusModel, node::Node};

#[derive(Debug)]
pub struct CircleMergingQuadtree<C: CircleRadiusModel> {
    head: Box<Node>,
    circle_radius_model: C,
    max_items_per_node: usize,
}

pub struct IntoIter {
    stack: Vec<Node>,
}

pub struct Iter<'t> {
    stack: Vec<&'t Node>,
    output: Vec<CircleOfPoints>,
}

pub struct RectangleIter<'a> {
    stack: VecDeque<&'a Node>,
}

impl<C> CircleMergingQuadtree<C>
where
    C: CircleRadiusModel,
{
    pub fn new(bounds: BoundingBox2D, circle_radius_model: C, max_items_per_node: usize) -> Self {
        CircleMergingQuadtree {
            head: Box::new(Node::new(bounds)),
            circle_radius_model,
            max_items_per_node,
        }
    }

    pub fn insert(&mut self, coordinate: &Coordinate2D) {
        let new_circle = CircleOfPoints::new_with_one_point(
            Circle::from_coordinate(coordinate, self.circle_radius_model.min_radius()),
            Default::default(), // TODO: allow inserting attribute data
        );

        self.insert_circle(new_circle);
    }

    /// Use the output of a tree as input to another one.
    ///
    /// This is useful to build a lower resolution tree upon existing circles.
    pub fn insert_tree(&mut self, tree: CircleMergingQuadtree<C>) {
        for circle in tree {
            self.insert_circle(circle);
        }
    }

    pub fn insert_circle(&mut self, new_circle: CircleOfPoints) {
        let mut insert_attempt = self.head.try_insert(
            new_circle,
            self.circle_radius_model.delta(),
            self.max_items_per_node,
        );

        while let Err((circle1, circle2)) = insert_attempt {
            let mut new_circle = circle1;
            new_circle.merge(&circle2, &self.circle_radius_model);
            insert_attempt = self.head.try_insert(
                new_circle,
                self.circle_radius_model.delta(),
                self.max_items_per_node,
            );
        }
    }

    pub fn iter_rectangles(&self) -> RectangleIter {
        let mut stack = VecDeque::new();
        stack.push_back(&*self.head);
        RectangleIter { stack }
    }
}

impl<C> IntoIterator for CircleMergingQuadtree<C>
where
    C: CircleRadiusModel,
{
    type Item = CircleOfPoints;
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            stack: vec![*self.head],
        }
    }
}

impl Iterator for IntoIter {
    type Item = CircleOfPoints;

    fn next(&mut self) -> Option<Self::Item> {
        let mut top_node = self.stack.pop();

        while let Some(mut node) = top_node {
            match node.circles.pop() {
                Some(circle) => {
                    self.stack.push(node);
                    return Some(circle);
                }
                None => {
                    if let Some(link) = node.link {
                        self.stack.append(&mut (link as Box<[_]>).into_vec());
                    }
                    top_node = self.stack.pop();
                }
            }
        }

        None
    }
}

impl<'t, C> IntoIterator for &'t CircleMergingQuadtree<C>
where
    C: CircleRadiusModel,
{
    type Item = CircleOfPoints;
    type IntoIter = Iter<'t>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            stack: vec![self.head.as_ref()],
            output: Vec::new(),
        }
    }
}

impl<'t> Iterator for Iter<'t> {
    type Item = CircleOfPoints;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(circle) = self.output.pop() {
            return Some(circle);
        }

        let mut top_node = self.stack.pop();

        while let Some(node) = top_node {
            self.output.append(&mut node.circles.clone());

            if let Some(ref quad_reference) = node.link {
                self.stack
                    .extend_from_slice(&quad_reference.iter().collect::<Vec<&Node>>());
            }

            if self.output.is_empty() {
                top_node = self.stack.pop();
            } else {
                return self.output.pop();
            }
        }

        None
    }
}

impl<'a> Iterator for RectangleIter<'a> {
    type Item = &'a BoundingBox2D;

    fn next(&mut self) -> Option<Self::Item> {
        self.stack.pop_front().map(|node| {
            if let Some(ref link) = node.link {
                for child_node in link.iter() {
                    self.stack.push_back(child_node);
                }
            }

            &node.rectangle
        })
    }
}

#[cfg(test)]
mod test {
    use crate::processing::circle_merging_quadtree::circle_radius_model::LogScaledRadius;

    use super::*;
    use std::{cmp::Ordering, num::NonZeroUsize};

    #[test]
    fn insert_single_item() {
        let circle_radius_model = LogScaledRadius::new(5.0, 1.0).unwrap();
        let mut tree = CircleMergingQuadtree::new(
            BoundingBox2D::new_from_center(Coordinate2D::new(100.0, 100.0), 100.0, 100.0).unwrap(),
            circle_radius_model,
            1,
        );

        let p1 = Coordinate2D::new(0.0, 0.0);
        tree.insert(&p1);

        let mut iter = tree.into_iter();
        assert_eq!(
            Some(CircleOfPoints::new(Circle::new(0.0, 0.0, 5.0), 1, Default::default()).unwrap()),
            iter.next()
        );

        assert_eq!(None, iter.next());
    }

    #[test]
    fn insert_two_items_merge() {
        let points = vec![Coordinate2D::new(50.0, 50.0), Coordinate2D::new(50.0, 50.0)];
        let points_size = NonZeroUsize::new(points.len()).unwrap();

        let circle_radius_model = LogScaledRadius::new(5.0, 1.0).unwrap();
        let mut tree = CircleMergingQuadtree::new(
            BoundingBox2D::new_from_center(Coordinate2D::new(100.0, 100.0), 100.0, 100.0).unwrap(),
            circle_radius_model,
            1,
        );

        for p in &points {
            tree.insert(p);
        }

        let mut iter = tree.into_iter();
        assert_eq!(
            Some(
                CircleOfPoints::new(
                    Circle::new(
                        50.0,
                        50.0,
                        circle_radius_model.calculate_radius(points_size)
                    ),
                    points_size.get(),
                    Default::default()
                )
                .unwrap()
            ),
            iter.next()
        );

        assert_eq!(None, iter.next());
    }

    #[test]
    fn insert_two_items_merge_with_move() {
        let points = vec![Coordinate2D::new(48.0, 48.0), Coordinate2D::new(50.0, 50.0)];
        let points_size = NonZeroUsize::new(points.len()).unwrap();

        let circle_radius_model = LogScaledRadius::new(5.0, 1.0).unwrap();
        let mut tree = CircleMergingQuadtree::new(
            BoundingBox2D::new_from_center(Coordinate2D::new(100.0, 100.0), 100.0, 100.0).unwrap(),
            circle_radius_model,
            1,
        );

        for p in &points {
            tree.insert(p);
        }

        let mut iter = tree.into_iter();
        assert_eq!(
            Some(
                CircleOfPoints::new(
                    Circle::new(
                        49.0,
                        49.0,
                        circle_radius_model.calculate_radius(points_size)
                    ),
                    points_size.get(),
                    Default::default()
                )
                .unwrap()
            ),
            iter.next()
        );

        assert_eq!(None, iter.next());
    }

    #[test]
    fn insert_five_items_merge() {
        let points = vec![
            Coordinate2D::new(20.0, 50.0),
            Coordinate2D::new(20.0, 50.0),
            Coordinate2D::new(20.0, 50.0),
            Coordinate2D::new(20.0, 50.0),
            Coordinate2D::new(20.0, 50.0),
        ];
        let points_size = points.len();

        let circle_radius_model = LogScaledRadius::new(5.0, 1.0).unwrap();
        let mut tree = CircleMergingQuadtree::new(
            BoundingBox2D::new_from_center(Coordinate2D::new(100.0, 100.0), 100.0, 100.0).unwrap(),
            circle_radius_model,
            1,
        );

        for p in &points {
            tree.insert(p);
        }

        let mut iter = tree.into_iter();
        assert_eq!(
            Some(
                CircleOfPoints::new(
                    Circle::new(
                        20.0,
                        50.0,
                        circle_radius_model.calculate_radius(NonZeroUsize::new(5).unwrap())
                    ),
                    points_size,
                    Default::default()
                )
                .unwrap()
            ),
            iter.next()
        );

        assert_eq!(None, iter.next());
    }

    #[test]
    fn insert_two_items_no_merge() {
        let points = vec![Coordinate2D::new(50.0, 50.0), Coordinate2D::new(75.0, 75.0)];

        let circle_radius_model = LogScaledRadius::new(5.0, 1.0).unwrap();
        let mut tree = CircleMergingQuadtree::new(
            BoundingBox2D::new_from_center(Coordinate2D::new(100.0, 100.0), 100.0, 100.0).unwrap(),
            circle_radius_model,
            1,
        );

        for p in &points {
            tree.insert(p);
        }

        let mut results = Vec::with_capacity(2);
        for circle in tree {
            results.push(circle);
        }
        results.sort_by(|a, b| match a.circle.x().partial_cmp(&b.circle.x()) {
            Some(Ordering::Greater | Ordering::Less) => Ordering::Greater,
            _ => a
                .circle
                .y()
                .partial_cmp(&b.circle.y())
                .unwrap_or(Ordering::Equal),
        });

        let mut iter = results.into_iter();
        assert_eq!(
            Some(CircleOfPoints::new(Circle::new(50.0, 50.0, 5.0), 1, Default::default()).unwrap(),),
            iter.next()
        );
        assert_eq!(
            Some(CircleOfPoints::new(Circle::new(75.0, 75.0, 5.0), 1, Default::default()).unwrap(),),
            iter.next()
        );

        assert_eq!(None, iter.next());
    }

    #[test]
    fn bounding_rectangle() {
        let bounds =
            BoundingBox2D::new_from_center(Coordinate2D::new(100.0, 100.0), 100.0, 100.0).unwrap();

        let circle_radius_model = LogScaledRadius::new(5.0, 1.0).unwrap();
        let tree = CircleMergingQuadtree::new(bounds, circle_radius_model, 1);

        let mut iter = tree.iter_rectangles();
        assert_eq!(Some(&bounds), iter.next());

        assert_eq!(None, iter.next());
    }

    #[test]
    fn bounding_rectangle_after_split() {
        let bounds =
            BoundingBox2D::new_from_center(Coordinate2D::new(100.0, 100.0), 100.0, 100.0).unwrap();

        let points = vec![
            Coordinate2D::new(150.0, 150.0),
            Coordinate2D::new(50.0, 50.0),
        ];

        let circle_radius_model = LogScaledRadius::new(5.0, 1.0).unwrap();
        let mut tree = CircleMergingQuadtree::new(bounds, circle_radius_model, 1);

        for point in &points {
            tree.insert(point);
        }

        let bounding_boxes = tree.iter_rectangles().collect::<Vec<_>>();

        assert_eq!(
            bounding_boxes,
            vec![
                &bounds,
                &BoundingBox2D::new_from_center(Coordinate2D::new(50.0, 50.0), 50.0, 50.0).unwrap(),
                &BoundingBox2D::new_from_center(Coordinate2D::new(150.0, 50.0), 50.0, 50.0)
                    .unwrap(),
                &BoundingBox2D::new_from_center(Coordinate2D::new(50.0, 150.0), 50.0, 50.0)
                    .unwrap(),
                &BoundingBox2D::new_from_center(Coordinate2D::new(150.0, 150.0), 50.0, 50.0)
                    .unwrap(),
            ]
        );
    }
}
