use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayData, BooleanArray, Int32Array, Int32Builder, ListArray, PrimitiveArray,
        StringArray, StructArray,
    },
    buffer::Buffer,
    datatypes::{DataType, Date64Type, Field, Time64NanosecondType, ToByteSlice},
};

fn main() {
    // 原始数组:

    // 基本数组是固定宽度基本类型的数组 (bool, u8, i8, u16, i16, u32, i32, u64, i64, f32, f64)

    // 创建一个容量为100的有符号的 32 位整数数组构建器。
    let mut primitive_array_builder = Int32Builder::with_capacity(100);

    // 附加一个单独的原始值
    primitive_array_builder.append_value(55);

    // 附加一个空值
    primitive_array_builder.append_null();

    // 附加原始值的切片
    primitive_array_builder.append_slice(&[39, 89, 12]);

    // 添加大量的值
    primitive_array_builder.append_null();
    primitive_array_builder.append_slice(&(25..50).collect::<Vec<i32>>());

    // 构建原始数组
    let primitive_array = primitive_array_builder.finish();

    // 长数组将在中间打印一个省略号
    println!("{primitive_array:?}");

    // 数组也可以从Vec<Option<T>> 构建。“None”表示数组中的空值。
    let date_array: PrimitiveArray<Date64Type> =
        vec![Some(1550902545147), None, Some(1550902545147)].into();
    println!("{date_array:?}");

    let time_array: PrimitiveArray<Time64NanosecondType> = (0..100).collect::<Vec<i64>>().into();
    println!("{time_array:?}");

    // 我们可以直接从底层缓冲区构建数组。

    // BinaryArray是字节数组的数组，其中每个字节数组是底层缓冲区的一个切片。
    let values: [u8; 12] = [
        b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
    ];
    let offsets: [i32; 4] = [0, 5, 5, 12];

    let array_data = ArrayData::builder(DataType::Utf8)
        .len(3)
        .add_buffer(Buffer::from(offsets.to_byte_slice()))
        .add_buffer(Buffer::from(&values))
        .null_bit_buffer(Some(Buffer::from([0b00000101])))
        .build()
        .unwrap();

    let binary_array = StringArray::from(array_data);
    println!("{binary_array:?}");

    // ListArrays are similar to ByteArrays: they are arrays of other
    // arrays, where each child array is a slice of the underlying
    // buffer.
    let value_data = ArrayData::builder(DataType::Int32)
        .len(8)
        .add_buffer(Buffer::from([0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
        .build()
        .unwrap();

    // Construct a buffer for value offsets, for the nested array:
    //  [[0, 1, 2], [3, 4, 5], [6, 7]]
    let value_offsets = Buffer::from([0, 3, 6, 8].to_byte_slice());

    // Construct a list array from the above two
    let list_data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false)));
    let list_data = ArrayData::builder(list_data_type)
        .len(3)
        .add_buffer(value_offsets)
        .add_child_data(value_data)
        .build()
        .unwrap();
    let list_array = ListArray::from(list_data);

    println!("{list_array:?}");

    // StructArrays can be constructed using the StructArray::from
    // helper, which takes the underlying arrays and field types.
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            Arc::new(BooleanArray::from(vec![false, false, true, true])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
        ),
    ]);
    println!("{struct_array:?}");
}
