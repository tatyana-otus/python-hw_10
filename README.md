# Protobuf Serializer
##  Задание: дописать extension (pb.c):
* deviceapps_xwrite_pb должна принимать на вход iterable со словарями и путь до файла, писать запакованные в
protobuf словари в файл, сжав gzip'ом. Каждая protobuf сообщенька должна идти вместе с заголовком,
определенным pbheader_t. Функция возвращает количество записанных байт.
* [опционально] deviceapps_xread_pb читает записанные deviceapps_xwrite_pb сообщеньки из файла в виде
словарей. Возвращает генератор со словарями. https://eli.thegreenplace.net/2012/04/05/implementing‐a‐
generatoryield‐in‐a‐python‐c‐extension
* расширить и дописать тесты соответствующим образом.

## Usage
```
git clone https://github.com/tatyana-otus/python-hw_10.git
cd python-hw_10/
docker run -w /tmp/otus -ti --rm -v "$(pwd)":/tmp/otus/ centos /bin/bash
./start.sh
```