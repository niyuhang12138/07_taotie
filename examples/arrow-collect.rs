use arrow::{
    array::{Array, Int8Array, ListArray},
    datatypes::Int32Type,
};

fn main() {
    let array: Int8Array = vec![1, 2, 3, 4, 5].into_iter().collect();
    println!("{:?}", array);

    let array: Int8Array = vec![Some(1), Some(2), None, Some(3)].into_iter().collect();
    println!("{:?}", array);
    assert!(array.is_null(2));

    let data = vec![
        Some(vec![]),
        None,
        Some(vec![Some(3), None, Some(5), Some(19)]),
        Some(vec![Some(6), Some(7)]),
    ];
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
    println!("{:?}", list_array);
}
