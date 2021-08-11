use geoengine_datatypes::primitives::BoundingBox2D;

use super::circle_of_points::CircleOfPoints;

pub type QuadReference = [Node; 4];

///  * `top_left`
///  * `top_right`
///  * `bottom_left`
///  * `bottom_right`
pub type Link = Option<Box<QuadReference>>;

#[derive(Debug)]
pub struct Node {
    pub rectangle: BoundingBox2D,
    pub circles: Vec<CircleOfPoints>,
    pub link: Link,
}

pub type TryResult = Result<(), (CircleOfPoints, CircleOfPoints)>;
type PartialTryResult = Option<CircleOfPoints>;

impl Node {
    pub fn new(rectangle: BoundingBox2D) -> Self {
        Node {
            rectangle,
            circles: Vec::new(),
            link: None,
        }
    }

    pub fn add(
        &mut self,
        circle: CircleOfPoints,
        epsilon_distance: f64,
        max_items_per_node: usize,
    ) {
        if let Some(ref mut link) = self.link {
            // node is splitted

            for section in link.iter_mut() {
                if section
                    .rectangle
                    .contains_with_delta(&circle.circle, epsilon_distance)
                {
                    section.add(circle, epsilon_distance, max_items_per_node);
                    // return;
                    unreachable!();
                }
            }

            self.circles.push(circle);
        } else {
            // no is unsplitted

            self.circles.push(circle);

            // handle overflow
            if self.circles.len() > max_items_per_node {
                let (top_left_rect, top_right_rect, bottom_left_rect, bottom_right_rect) =
                    self.rectangle.split_into_quarters();

                let mut link = Box::new([
                    Node::new(top_left_rect),
                    Node::new(top_right_rect),
                    Node::new(bottom_left_rect),
                    Node::new(bottom_right_rect),
                ]);

                'circle: for circle in std::mem::take(&mut self.circles) {
                    for section in link.iter_mut() {
                        if section
                            .rectangle
                            .contains_with_delta(&circle.circle, epsilon_distance)
                        {
                            section.add(circle, epsilon_distance, max_items_per_node);
                            continue 'circle;
                        }
                    }

                    self.circles.push(circle);
                }

                self.link = Some(link);
            }
        }
    }

    pub fn try_insert(
        &mut self,
        mut new_circle: CircleOfPoints,
        epsilon_distance: f64,
        max_items_per_node: usize,
    ) -> TryResult {
        // overlap with a contained circle
        if let Some(circle) = self.remove_intersecting_circle(&new_circle, epsilon_distance) {
            return Err((new_circle, circle));
        }

        // try to insert in children
        if let Some(ref mut quad) = self.link {
            let result =
                Node::try_insert_children(quad, new_circle, epsilon_distance, max_items_per_node);

            match result {
                Ok(try_result) => {
                    return try_result;
                }
                Err(returned_new_circle) => {
                    new_circle = returned_new_circle;

                    // try to find overlap in intersecting child nodes
                    for section in quad.iter_mut() {
                        if section
                            .rectangle
                            .intersects_with_delta(&new_circle.circle, epsilon_distance)
                        {
                            if let Some(circle) = section.try_partial(&new_circle, epsilon_distance)
                            {
                                return Err((new_circle, circle));
                            }
                        }
                    }
                }
            }
        }

        self.add(new_circle, epsilon_distance, max_items_per_node);
        Ok(())
    }

    #[inline]
    fn try_insert_children(
        quad: &mut QuadReference,
        new_circle: CircleOfPoints,
        epsilon_distance: f64,
        max_items_per_node: usize,
    ) -> Result<TryResult, CircleOfPoints> {
        for section in quad.iter_mut() {
            if section
                .rectangle
                .contains_with_delta(&new_circle.circle, epsilon_distance)
            {
                return Ok(section.try_insert(new_circle, epsilon_distance, max_items_per_node));
            }
        }
        Err(new_circle)
    }

    fn try_partial(
        &mut self,
        new_circle: &CircleOfPoints,
        epsilon_distance: f64,
    ) -> PartialTryResult {
        if let Some(circle) = self.remove_intersecting_circle(new_circle, epsilon_distance) {
            return Some(circle);
        }

        if let Some(quad) = self.link.as_mut() {
            for section in quad.iter_mut() {
                if section
                    .rectangle
                    .intersects_with_delta(&new_circle.circle, epsilon_distance)
                {
                    if let Some(circle) = section.try_partial(new_circle, epsilon_distance) {
                        return Some(circle);
                    }
                }
            }
        }

        None
    }

    fn remove_intersecting_circle(
        &mut self,
        test_circle: &CircleOfPoints,
        epsilon_distance: f64,
    ) -> Option<CircleOfPoints> {
        let mut remove_index = None;
        for (i, circle) in self.circles.iter().enumerate() {
            if circle
                .circle
                .intersects_with_epsilon(&test_circle.circle, epsilon_distance)
            {
                remove_index = Some(i);
                break;
            }
        }

        remove_index.map(|i| self.circles.remove(i))
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.rectangle == other.rectangle
    }
}
