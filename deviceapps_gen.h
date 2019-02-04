#include <Python.h>
#include <stdio.h>
#include <zlib.h>
#include "const.h"


typedef struct {
    PyObject_HEAD
    gzFile file;
    uint8_t* message_buf;
    uint32_t* apps_buf;
} DeviceAppsGenState;


static PyObject* c_message_to_py(DeviceApps* c_msg)
{
    DeviceApps__Device* c_device = c_msg->device;

    PyObject* py_device_apps = PyDict_New();
    PyObject* py_device = PyDict_New();
    PyObject* py_apps_list = PyList_New(c_msg->n_apps);
    if(py_device_apps == NULL || py_device == NULL || py_apps_list == NULL ){
        PyErr_Format(PyExc_MemoryError, "Can't alloc memory");
        return NULL;
    }

    if (c_device->has_type){
        PyObject* py_type = Py_BuildValue("s#", c_device->type.data,
                                                c_device->type.len);
        PyDict_SetItemString(py_device, "type", py_type);
        Py_DECREF(py_type);
    }
    if (c_device->has_id){
        PyObject* py_id = Py_BuildValue("s#", c_device->id.data,
                                              c_device->id.len);
        PyDict_SetItemString(py_device, "id", py_id);
        Py_DECREF(py_id);
    }
    PyDict_SetItemString(py_device_apps, "device", py_device);
    Py_DECREF(py_device);

    if (c_msg->has_lat){
        PyObject* py_lat = Py_BuildValue("d", c_msg->lat);
        PyDict_SetItemString(py_device_apps, "lat", py_lat);
        Py_DECREF(py_lat);
    }
    if (c_msg->has_lon){
        PyObject* py_lon = Py_BuildValue("d", c_msg->lon);
        PyDict_SetItemString(py_device_apps, "lon", py_lon);
        Py_DECREF(py_lon);
    }
    for(int i = 0; i < c_msg->n_apps; ++i){
        PyList_SetItem(py_apps_list, i, Py_BuildValue("i", c_msg->apps[i]));
    }
    PyDict_SetItemString(py_device_apps, "apps", py_apps_list);
    Py_DECREF(py_apps_list);

    return py_device_apps;
}


static void
DeviceAppsGen_dealloc(DeviceAppsGenState *gstate)
{
    free(gstate->message_buf);
    free(gstate->apps_buf);
    gzclose(gstate->file);
    Py_TYPE(gstate)->tp_free(gstate);
}


static PyObject *
DeviceAppsGen_next(DeviceAppsGenState *gstate)
{
    pbheader_t header;
    DeviceApps* msg;
    PyObject* py_msg;

    while(gzread(gstate->file, &header, sizeof(header)) == sizeof(header)){
        if(header.magic != MAGIC){
            return NULL;
        }
        uint16_t msg_len  = header.length;
        if(msg_len > MAX_MESSAGE_SIZE) {
            PyErr_Format(PyExc_BufferError, "The message size exceeds %d", MAX_MESSAGE_SIZE);
            return NULL;
        }
        int read_bytes = gzread(gstate->file, gstate->message_buf, msg_len);
        if(read_bytes != msg_len){
            PyErr_Format(PyExc_RuntimeError, "Wrong file format");
            return NULL;
        }
        if(header.type == DEVICE_APPS_TYPE){
            msg = device_apps__unpack(NULL, msg_len, gstate->message_buf);
            if (msg == NULL){
                PyErr_Format(PyExc_RuntimeError, "Error unpacking");
                return NULL;
            }
            py_msg = c_message_to_py(msg);
            device_apps__free_unpacked(msg, NULL);
            return py_msg;
        }
    }
    return NULL;
}


PyObject *
DeviceAppsGen_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{   
    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    DeviceAppsGenState* gstate = (DeviceAppsGenState*)type->tp_alloc(type, 0);
    if (!gstate)
        return NULL;

    gstate->message_buf = malloc(MAX_MESSAGE_SIZE);
    gstate->apps_buf = malloc(MAX_APP_SIZE * sizeof(uint32_t));
    if (!gstate->message_buf || !gstate->apps_buf) {
        PyErr_Format(PyExc_MemoryError, "Can't alloc memory");
        return NULL;
    }

    gzFile f_h = gzopen(path, "rb");
    if(!f_h){
        free(gstate->message_buf);
        free(gstate->apps_buf);
        Py_TYPE(gstate)->tp_free(gstate);
        PyErr_Format(PyExc_OSError, "Can't open file");
        return NULL;
    }
    gstate->file = f_h;
    return (PyObject *)gstate;
}


PyTypeObject DeviceAppsGen_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "DeviceAppsGen",                   /* tp_name */
    sizeof(DeviceAppsGenState),        /* tp_basicsize */
    0,                                 /* tp_itemsize */
    (destructor)DeviceAppsGen_dealloc, /* tp_dealloc */
    0,                                 /* tp_print */
    0,                                 /* tp_getattr */
    0,                                 /* tp_setattr */
    0,                                 /* tp_reserved */
    0,                                 /* tp_repr */
    0,                                 /* tp_as_number */
    0,                                 /* tp_as_sequence */
    0,                                 /* tp_as_mapping */
    0,                                 /* tp_hash */
    0,                                 /* tp_call */
    0,                                 /* tp_str */
    0,                                 /* tp_getattro */
    0,                                 /* tp_setattro */
    0,                                 /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                /* tp_flags */
    0,                                 /* tp_doc */
    0,                                 /* tp_traverse */
    0,                                 /* tp_clear */
    0,                                 /* tp_richcompare */
    0,                                 /* tp_weaklistoffset */
    PyObject_SelfIter,                 /* tp_iter */
    (iternextfunc)DeviceAppsGen_next,  /* tp_iternext */
    0,                                 /* tp_methods */
    0,                                 /* tp_members */
    0,                                 /* tp_getset */
    0,                                 /* tp_base */
    0,                                 /* tp_dict */
    0,                                 /* tp_descr_get */
    0,                                 /* tp_descr_set */
    0,                                 /* tp_dictoffset */
    0,                                 /* tp_init */
    PyType_GenericAlloc,               /* tp_alloc */
    DeviceAppsGen_new,                 /* tp_new */
};
