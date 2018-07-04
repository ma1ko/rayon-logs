//! Store a trace as a fork join graph (in a vector).
use {svg::COLORS, Rectangle, TaskId, TaskLog};
type BlockId = usize;
use std::collections::HashMap;
use std::iter::repeat;
use RunLog;

const VERTICAL_GAP: f64 = 0.2;

use svg::Point;
use TimeStamp;

trait BlockVector {
    fn push_block(&mut self, block: Block) -> BlockId;
    fn sequence(&mut self, id: BlockId) -> &mut Vec<BlockId>;
    fn parallel(&mut self, id: BlockId) -> &mut Vec<BlockId>;
    fn add_task(&mut self, task: TaskLog) -> BlockId;
    fn add_sequence(&mut self) -> BlockId;
    fn add_parallel(&mut self) -> BlockId;
}

impl BlockVector for Vec<Block> {
    fn add_task(&mut self, task: TaskLog) -> BlockId {
        self.push_block(Block::Task(task))
    }

    fn add_sequence(&mut self) -> BlockId {
        self.push_block(Block::Sequence(Vec::new()))
    }

    fn add_parallel(&mut self) -> BlockId {
        self.push_block(Block::Parallel(Vec::new()))
    }

    fn push_block(&mut self, block: Block) -> BlockId {
        self.push(block);
        self.len() - 1
    }
    fn sequence(&mut self, id: BlockId) -> &mut Vec<BlockId> {
        match self[id] {
            Block::Sequence(ref mut s) => s,
            _ => panic!("should be a sequence"),
        }
    }
    fn parallel(&mut self, id: BlockId) -> &mut Vec<BlockId> {
        match self[id] {
            Block::Parallel(ref mut s) => s,
            _ => panic!("should be a parallel block"),
        }
    }
}

#[derive(Debug)]
enum Block {
    Task(TaskLog),
    Sequence(Vec<BlockId>),
    Parallel(Vec<BlockId>),
}

/// Create a fork join graph (stored in a vec). This is used to convert the logs into
/// a graphical display of animated rectangles.
fn create_graph(tasks: &[TaskLog]) -> Vec<Block> {
    let mut graph = vec![Block::Sequence(Vec::new())];

    // ok let's start now
    let mut fathers: HashMap<BlockId, BlockId> = HashMap::new();
    let mut current_blocks: HashMap<TaskId, BlockId> = HashMap::new();
    current_blocks.insert(0, 0); // init task and all its descendants go in initial sequence

    // we sort by starting time to be sure fathers are processed before children
    let mut sorted_tasks: Vec<TaskId> = (0..tasks.len()).collect();
    sorted_tasks.sort_unstable_by(|t1, t2| {
        tasks[*t1]
            .start_time
            .partial_cmp(&tasks[*t2].start_time)
            .unwrap()
    });

    for task_id in &sorted_tasks {
        let task = &tasks[*task_id];
        let current_block = *current_blocks
            .get(task_id)
            .expect(&format!("task {} is not created by anyone", task_id));
        // add task to its sequence
        let new_block = graph.add_task((*task).clone());
        graph.sequence(current_block).push(new_block);

        // now look at the children
        if task.children.len() == 1 {
            let child = task.children[0];
            let possible_existing_block = current_blocks.remove(&child);
            if possible_existing_block.is_some() {
                // hard case, this child has several fathers.
                current_blocks.insert(child, fathers[&current_block]);
            } else {
                //easy case, first time child is seen by a father. maybe he has only one.
                current_blocks.insert(child, current_block);
            }
        } else if !task.children.is_empty() {
            // several children, we create a parallel block
            let parallel_block = graph.add_parallel();
            // add it to our block
            graph.sequence(current_block).push(parallel_block);
            // now each child will execute in a different sequence we create now
            for child in &task.children {
                let sequential_block = graph.add_sequence();
                graph.parallel(parallel_block).push(sequential_block);
                let should_be_none = current_blocks.insert(*child, sequential_block);
                assert!(should_be_none.is_none());

                fathers.insert(sequential_block, current_block); // store where to go back at end of sequence
            }
        }
    }
    graph
}

/// recursively compute widths and heights of block at given index and all its sub-blocks.
fn compute_blocks_dimensions(
    index: BlockId,
    graph: &[Block],
    blocks_dimensions: &mut [(f64, f64)],
) -> (f64, f64) {
    let dimensions = match graph[index] {
        Block::Sequence(ref s) => s.iter().fold((0.0, -VERTICAL_GAP), |dimensions, id| {
            let (width, height) = compute_blocks_dimensions(*id, &graph, blocks_dimensions);
            (
                if width > dimensions.0 {
                    width
                } else {
                    dimensions.0
                },
                height + dimensions.1 + VERTICAL_GAP,
            )
        }),
        Block::Parallel(ref p) => p.iter().fold((0.0, 0.0), |dimensions, id| {
            let (width, height) = compute_blocks_dimensions(*id, &graph, blocks_dimensions);
            (
                width + dimensions.0,
                if height > dimensions.1 {
                    height
                } else {
                    dimensions.1
                },
            )
        }),
        Block::Task(ref t) => ((t.end_time - t.start_time) as f64, 1.0),
    };
    blocks_dimensions[index] = dimensions;
    dimensions
}

/// Find x and y coordinates for each block.
fn compute_positions(
    index: BlockId,
    graph: &[Block],
    blocks_dimensions: &[(f64, f64)],
    positions: &mut [(f64, f64)],
) {
    match graph[index] {
        Block::Sequence(ref s) => {
            // If it's a sequence, we move along y
            s.iter().fold(positions[index].1, |y, id| {
                // center on x
                let x_gap = (blocks_dimensions[index].0 - blocks_dimensions[*id].0) / 2.0;
                positions[*id] = (positions[index].0 + x_gap, y);
                compute_positions(*id, &graph, &blocks_dimensions, positions);
                y + blocks_dimensions[*id].1 + VERTICAL_GAP
            });
        }
        Block::Parallel(ref p) => {
            // If it's a parallel bloc, we move along x
            p.iter().fold(positions[index].0, |x, id| {
                // center on y
                let y_gap = (blocks_dimensions[index].1 - blocks_dimensions[*id].1) / 2.0;
                positions[*id] = (x, positions[index].1 + y_gap);
                compute_positions(*id, &graph, &blocks_dimensions, positions);
                x + blocks_dimensions[*id].0
            });
        }
        _ => (),
    }
}

/// For some tasks we know the type and work.
/// We can therefore compute a speed of computation.
/// We figure out what is the max speed for each task type.
fn compute_speeds(tasks: &[TaskLog]) -> HashMap<usize, f64> {
    let mut speeds: HashMap<usize, f64> = HashMap::new();
    for task in tasks {
        if let Some((ref work_type, work_amount)) = task.work {
            let speed = work_amount as f64 / (task.end_time as f64 - task.start_time as f64);
            let existing_speed: f64 = speeds.get(work_type).cloned().unwrap_or(0.0);
            if speed > existing_speed {
                speeds.insert(*work_type, speed);
            }
        }
    }
    speeds
}

/// Take a block ; fill its rectangles and edges and return a set of entry points for incoming edges
/// and a set of exit points for outgoing edges.
fn generate_visualisation(
    index: BlockId,
    graph: &[Block],
    positions: &[(f64, f64)],
    speeds: &HashMap<usize, f64>,
    rectangles: &mut Vec<Rectangle>,
    edges: &mut Vec<(Point, Point)>,
) -> (Vec<Point>, Vec<Point>) {
    match graph[index] {
        Block::Sequence(ref s) => {
            let points: Vec<(Vec<Point>, Vec<Point>)> = s.iter()
                .map(|b| generate_visualisation(*b, graph, positions, speeds, rectangles, edges))
                .collect();
            edges.extend(
                points
                    .windows(2)
                    .flat_map(|w| iproduct!(w[0].1.iter(), w[1].0.iter()).map(|(a, b)| (*a, *b))),
            );
            (
                points.first().map(|p| &p.0).unwrap().clone(),
                points.last().map(|p| &p.1).unwrap().clone(),
            )
        }
        Block::Parallel(ref p) => p.iter().fold((Vec::new(), Vec::new()), |mut acc, b| {
            let (entry, exit) =
                generate_visualisation(*b, graph, positions, speeds, rectangles, edges);
            acc.0.extend(entry);
            acc.1.extend(exit);
            acc
        }),
        Block::Task(ref t) => {
            let duration = (t.end_time - t.start_time) as f64;
            rectangles.push(Rectangle::new(
                [0.0, 0.0, 0.0],
                1.0,
                positions[index],
                (duration, 1.0),
                None,
            ));
            let opacity = if let Some((work_type, work_amount)) = t.work {
                let speed = work_amount as f64 / duration;
                let best_speed = speeds[&work_type];
                0.4 + 0.6 * speed / best_speed
            } else {
                1.0
            };
            rectangles.push(Rectangle::new(
                COLORS[t.thread_id % COLORS.len()],
                opacity as f32,
                positions[index],
                (duration, 1.0),
                Some((t.start_time, t.end_time)),
            ));

            (
                vec![(positions[index].0 + duration / 2.0, positions[index].1)],
                vec![(
                    positions[index].0 + duration / 2.0,
                    positions[index].1 + 1.0,
                )],
            )
        }
    }
}

/// Take all taskslogs and compute idle periods animations for each thread.
/// add all rectangles to given vector.
/// given height (height of animated running tasks) enables us to center the display vertically.
/// y is vertical start for this log.
fn compute_idle_times(
    tasks: &[TaskLog],
    starting_position: &(f64, f64),
    threads_number: usize,
    rectangles: &mut Vec<Rectangle>,
) {
    // do one pass to figure out the last recorded time.
    // we need it to figure out who is idle at the end.
    let last_time = tasks.iter().map(|t| t.end_time).max().unwrap();
    let first_time = tasks.iter().map(|t| t.start_time).min().unwrap();

    // sort everyone by time (yes i know, again).
    // we add fake tasks at the end for last idle periods.
    let mut sorted_tasks: Vec<(usize, TimeStamp, TimeStamp)> = tasks
        .iter()
        .map(|t| (t.thread_id, t.start_time, t.end_time))
        .chain((0..threads_number).map(|i| (i, last_time, last_time + 1)))
        .collect();

    sorted_tasks.sort_by(|t1, t2| t1.1.partial_cmp(&t2.1).unwrap());

    let mut previous_activities: Vec<TimeStamp> = repeat(first_time).take(threads_number).collect();
    let mut current_x_positions: Vec<f64> =
        repeat(starting_position.0).take(threads_number).collect();

    // replay execution, figuring out idle times
    for (thread_id, start, end) in sorted_tasks {
        let previous_end = previous_activities[thread_id];
        if start > previous_end {
            let inactivity = (start - previous_end) as f64;
            rectangles.push(Rectangle::new(
                COLORS[thread_id % COLORS.len()],
                1.0,
                (
                    current_x_positions[thread_id],
                    starting_position.1 + thread_id as f64 * (1.0 + VERTICAL_GAP),
                ),
                (inactivity, 1.0),
                Some((previous_end, start)),
            ));
            current_x_positions[thread_id] += inactivity;
        }
        previous_activities[thread_id] = end;
    }
}

/// convert all tasks information into animated rectangles and edges.
pub fn visualisation<'a>(
    traces: impl Iterator<Item = &'a RunLog>,
) -> (Vec<Rectangle>, Vec<(Point, Point)>) {
    let mut rectangles = Vec::new();
    let mut edges = Vec::new();

    traces.fold(0.0, |y, log| {
        let tasks = &log.tasks_logs;
        let g = create_graph(tasks);

        // compute recursively the width and height of each block
        let mut blocks_dimensions = Vec::with_capacity(g.len());
        unsafe { blocks_dimensions.set_len(g.len()) }
        compute_blocks_dimensions(0, &g, &mut blocks_dimensions);

        // compute recursively the position of each block
        let mut positions = Vec::with_capacity(g.len());
        unsafe { positions.set_len(g.len()) }
        positions[0] = (0.0, y);
        compute_positions(0, &g, &blocks_dimensions, &mut positions);

        // adjust colors based on work
        let speeds = compute_speeds(tasks);

        // generate all rectangles and all edges
        generate_visualisation(0, &g, &positions, &speeds, &mut rectangles, &mut edges);

        // compute position for idle times widget
        let height = positions
            .iter()
            .map(|(_, y)| y)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap() + 1.0 - y;

        let width = positions
            .iter()
            .zip(blocks_dimensions.iter())
            .map(|((x, _), (w, _))| *x + *w)
            .max_by(|a, b| a.partial_cmp(&b).unwrap())
            .unwrap();

        let starting_position = (
            width as f64 * 1.02,
            y + (height - log.threads_number as f64) / 2.0,
        );

        compute_idle_times(
            tasks,
            &starting_position,
            log.threads_number,
            &mut rectangles,
        );
        y + height
    });
    // turn tasks into blocks (we build the fork join graph)
    (rectangles, edges)
}