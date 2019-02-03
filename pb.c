#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stddef.h>
#include <zlib.h>
#include "deviceapps.pb-c.h"
#include "deviceapps_gen.h"
#include "const.h"


static void addDoubleField(PyObject* d, protobuf_c_boolean* has,
                           double* field, const char* field_name)
{
    PyObject* py_field = PyDict_GetItemString(d, field_name);
    if(py_field != NULL){
        double value = PyFloat_AsDouble(py_field);
        *has = 1;
        *field = value;
    }
    else *has = 0;
}


static void addStringField(PyObject* dic, protobuf_c_boolean* has,
                           ProtobufCBinaryData* field,
                           const char* field_name)
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


static int addApps(PyObject* dic, DeviceApps* p_msg)
{
    PyObject* py_apps = PyDict_GetItemString(dic, "apps");
    if(PyList_Check(py_apps)){
        size_t c_apps_len = PyList_Size(py_apps);
        if(c_apps_len > MAX_APP_SIZE){
            PyErr_Format(PyExc_BufferError, "The number of applications exceeds %d", MAX_APP_SIZE);
            return -1;
        }
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


static void free_resources(uint8_t* msg, uint32_t* app, gzFile f_h)
{
    free(msg);
    free(app);
    gzclose(f_h);
}


static int py_message_to_c(PyObject* py_msg, DeviceApps* c_msg, DeviceApps__Device* c_device)
{
    PyObject* py_device = PyDict_GetItemString(py_msg, "device");
    if(py_device != NULL){
        addStringField(py_device, &c_device->has_type, &c_device->type, "type");
        addStringField(py_device, &c_device->has_id,   &c_device->id,   "id");
    }
    addDoubleField(py_msg, &c_msg->has_lat, &c_msg->lat, "lat");
    addDoubleField(py_msg, &c_msg->has_lon, &c_msg->lon, "lon");

    return addApps(py_msg, c_msg);
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

    gzFile f_h = gzopen(path, "ab9");
    if(!f_h){
        PyErr_Format(PyExc_OSError, "Can't create/open file");
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
    size_t wr_bytes = 0;
    for(size_t i = 0; i < size; ++i){
        PyObject* d = PyList_GetItem(obj, i);
        if(!PyDict_Check(d))
            continue;

        if (py_message_to_c(d, &msg, &device) == -1){
            free_resources(buf, apps_buf, f_h);
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


// Unpack only messages with type == DEVICE_APPS_TYPE
// Return iterator of Python dicts
static PyObject* py_deviceapps_xread_pb(PyObject* self, PyObject* args)
{
    return DeviceAppsGen_new(&DeviceAppsGen_Type, args, NULL);
}


static PyMethodDef PBMethods[] = {
     {"deviceapps_xwrite_pb", py_deviceapps_xwrite_pb, METH_VARARGS, "Write serialized protobuf to file fro iterator"},
     {"deviceapps_xread_pb", py_deviceapps_xread_pb, METH_VARARGS, "Deserialize protobuf from file, return iterator"},
     {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC initpb(void)
{
    PyObject *module = Py_InitModule("pb", PBMethods);
    if (!module)
        return;

    if (PyType_Ready(&DeviceAppsGen_Type) < 0)
        return;
    Py_INCREF((PyObject *)&DeviceAppsGen_Type);
    PyModule_AddObject(module, "DeviceAppsGen", (PyObject *)&DeviceAppsGen_Type);
}
