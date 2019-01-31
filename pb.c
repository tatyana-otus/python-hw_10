#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stddef.h>
#include <zlib.h>
#include "deviceapps.pb-c.h"


#define MAGIC  0xFFFFFFFF
#define DEVICE_APPS_TYPE 1

typedef struct pbheader_s {
    uint32_t magic;
    uint16_t type;
    uint16_t length;
} pbheader_t;
#define PBHEADER_INIT {MAGIC, DEVICE_APPS_TYPE, 0}


#define MAX_APP_SIZE 256
#define MAX_MESSAGE_SIZE 2*1024

// https://github.com/protobuf-c/protobuf-c/wiki/Examples
void example() {
    DeviceApps msg = DEVICE_APPS__INIT;
    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;
    void *buf;
    unsigned len;

    char *device_id = "e7e1a50c0ec2747ca56cd9e1558c0d7c";
    char *device_type = "idfa";
    device.has_id = 1;
    device.id.data = (uint8_t*)device_id;
    device.id.len = strlen(device_id);
    device.has_type = 1;
    device.type.data = (uint8_t*)device_type;
    device.type.len = strlen(device_type);
    msg.device = &device;

    msg.has_lat = 1;
    msg.lat = 67.7835424444;
    msg.has_lon = 1;
    msg.lon = -22.8044005471;

    msg.n_apps = 3;
    msg.apps = malloc(sizeof(uint32_t) * msg.n_apps);
    msg.apps[0] = 42;
    msg.apps[1] = 43;
    msg.apps[2] = 44;
    len = device_apps__get_packed_size(&msg);

    buf = malloc(len);
    device_apps__pack(&msg, buf);

    fprintf(stderr,"Writing %d serialized bytes\n",len); // See the length of message
    fwrite(buf, len, 1, stdout); // Write to stdout to allow direct command line piping

    free(msg.apps);
    free(buf);
}


void addDoubleField(PyObject* d, protobuf_c_boolean* has, double* field, const char* field_name)
{
    PyObject* py_field = PyDict_GetItemString(d, field_name);
    if(py_field != NULL){
        double value = PyFloat_AsDouble(py_field);
        *has = 1;
        *field = value;
    }
    else *has = 0;
}


void addStringField(PyObject* dic, protobuf_c_boolean* has, ProtobufCBinaryData* field, const char* field_name)
{
    PyObject* py_field = PyDict_GetItemString(dic, field_name);
    if(py_field != NULL){
        const char* value = PyString_AsString(py_field);
        *has = 1;
        field->data = (uint8_t*)value;
        field->len = strlen(value);
    }
    else *has = 0;
}


int addApps(PyObject* dic, DeviceApps* p_msg)
{
    PyObject* py_apps = PyDict_GetItemString(dic, "apps");
    if(PyList_Check(py_apps)){
        size_t c_apps_len = PyList_Size(py_apps);
        if(c_apps_len > MAX_APP_SIZE)
            return -1;
        p_msg->n_apps = c_apps_len;
        for(size_t j = 0; j < c_apps_len; ++j){
            PyObject* py_int = PyList_GetItem(py_apps, j);
            p_msg->apps[j] = PyLong_AsUnsignedLong(py_int);
        }
    }
    else {
        p_msg->n_apps = 0;
        p_msg->apps = NULL;
    }
    return 0;
}


void free_resources(uint8_t* msg, uint32_t* app, gzFile f_h)
{
    free(msg);
    free(app);
    gzclose(f_h);
}


static PyObject* py_deviceapps_xwrite_pb(PyObject* self, PyObject* args)
{
    const char* path;
    PyObject* obj;

    if (!PyArg_ParseTuple(args, "Os", &obj, &path))
        return NULL;
    if(!PyList_Check(obj))
        return NULL;

    size_t size = PyList_Size(obj);

    DeviceApps msg = DEVICE_APPS__INIT;
    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;

    size_t wr_bytes = 0;
    gzFile f_h = gzopen(path, "wb9");
    if(!f_h){
        PyErr_Format(PyExc_OSError, "Can't create file");
        return NULL;
    }
    uint8_t* buf = malloc(MAX_MESSAGE_SIZE + sizeof(pbheader_t));
    uint32_t* apps_buf = malloc(MAX_APP_SIZE * sizeof(uint32_t));
    if (!buf || !apps_buf) {
        PyErr_Format(PyExc_MemoryError, "Can't alloc memory");
        return NULL;
    }
    msg.device = &device;
    msg.apps = apps_buf;
    pbheader_t* buf_header = (pbheader_t*)buf;
    uint8_t* buf_message = buf + sizeof(pbheader_t);
    buf_header->magic = MAGIC;
    buf_header->type = DEVICE_APPS_TYPE;
    for(size_t i = 0; i < size; ++i){
        PyObject* d = PyList_GetItem(obj, i);
        if(!PyDict_Check(d))
            continue;
        PyObject* py_device = PyDict_GetItemString(d, "device");
        if(py_device != NULL){
            addStringField(py_device, &device.has_type, &device.type, "type");
            addStringField(py_device, &device.has_id, &device.id, "id");
        }
        addDoubleField(d, &msg.has_lat, &msg.lat, "lat");
        addDoubleField(d, &msg.has_lon, &msg.lon, "lon");
        if(addApps(d, &msg) == -1){
            free_resources(buf, apps_buf, f_h);
            PyErr_Format(PyExc_BufferError, "The number of applications exceeds %d", MAX_APP_SIZE);
            return NULL;
        }
        size_t len = device_apps__get_packed_size(&msg);
        if(len > MAX_MESSAGE_SIZE) {
            free_resources(buf, apps_buf, f_h);
            PyErr_Format(PyExc_BufferError, "The message size exceeds %d", MAX_MESSAGE_SIZE);
            return NULL;
        }
        buf_header->length = len;
        device_apps__pack(&msg, buf_message);
        gzwrite(f_h, buf, sizeof(pbheader_t) + len);
        wr_bytes += sizeof(pbheader_t) + len;
    }
    free_resources(buf, apps_buf, f_h);

    printf("Write to: %s\n", path);

    return Py_BuildValue("i", wr_bytes);
}


void c_message_to_py(DeviceApps* c_msg, PyObject* py_msg)
{
    DeviceApps__Device* c_device = c_msg->device;

    PyObject* py_device_apps = PyDict_New();
    PyObject* py_device = PyDict_New();
    PyObject* py_apps_list = PyList_New(c_msg->n_apps); // check

    if (c_device->has_type){
        PyDict_SetItemString(py_device, "type", Py_BuildValue("s#", c_device->type.data,
                                                                    c_device->type.len));
    }
    if (c_device->has_id){
        PyDict_SetItemString(py_device, "id", Py_BuildValue("s#", c_device->id.data,
                                                                  c_device->id.len));
    }
    PyDict_SetItemString(py_device_apps, "device", py_device);
    Py_DECREF(py_device);

    if (c_msg->has_lat){
        PyDict_SetItemString(py_device_apps, "lat", Py_BuildValue("d", c_msg->lat));
    }
    if (c_msg->has_lon){
        PyDict_SetItemString(py_device_apps, "lon", Py_BuildValue("d", c_msg->lon));
    }
    for(int i = 0; i < c_msg->n_apps; ++i){
        PyList_SetItem(py_apps_list, i, Py_BuildValue("i", c_msg->apps[i]));
    }
    PyDict_SetItemString(py_device_apps, "apps", py_apps_list);
    Py_DECREF(py_apps_list);

    PyList_Append(py_msg, py_device_apps);
}

// Unpack only messages with type == DEVICE_APPS_TYPE
// Return iterator of Python dicts
static PyObject* py_deviceapps_xread_pb(PyObject* self, PyObject* args) {
    const char* path;
    pbheader_t header;
    DeviceApps* msg;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    gzFile f_h = gzopen(path, "rb");
    if(!f_h){
        PyErr_Format(PyExc_OSError, "Can't open file");
        return NULL;
    }

    uint8_t* buf = malloc(MAX_MESSAGE_SIZE);
    uint32_t* apps_buf = malloc(MAX_APP_SIZE * sizeof(uint32_t));
    if (!buf || !apps_buf) {
        PyErr_Format(PyExc_MemoryError, "Can't alloc memory");
        return NULL;
    }

    PyObject* py_message = PyList_New(0);

    while(gzread(f_h, &header, sizeof(header))){
        if(header.magic != MAGIC){
            free_resources(buf, apps_buf, f_h);
            return NULL;
        }
        uint16_t msg_len  = header.length;
        if(msg_len > MAX_MESSAGE_SIZE) {
            free_resources(buf, apps_buf, f_h);
            PyErr_Format(PyExc_BufferError, "The message size exceeds %d", MAX_MESSAGE_SIZE);
            return NULL;
        }
        int read_bytes = gzread(f_h, buf, msg_len);
        if(read_bytes != msg_len){
            free_resources(buf, apps_buf, f_h);
            return NULL;
        }
        if(header.type == DEVICE_APPS_TYPE){
            msg = device_apps__unpack(NULL, msg_len, buf);
            if (msg == NULL){
                PyErr_Format(PyExc_RuntimeError, "Error unpacking");
                free_resources(buf, apps_buf, f_h);
                return NULL;
            }
            c_message_to_py(msg, py_message);
            device_apps__free_unpacked(msg, NULL);
        }
    }
    free_resources(buf, apps_buf, f_h);
    printf("Read from: %s\n", path);
    return py_message;
}


static PyMethodDef PBMethods[] = {
     {"deviceapps_xwrite_pb", py_deviceapps_xwrite_pb, METH_VARARGS, "Write serialized protobuf to file fro iterator"},
     {"deviceapps_xread_pb", py_deviceapps_xread_pb, METH_VARARGS, "Deserialize protobuf from file, return iterator"},
     {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC initpb(void) {
     (void) Py_InitModule("pb", PBMethods);
}
