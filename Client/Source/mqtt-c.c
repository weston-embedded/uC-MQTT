/*
*********************************************************************************************************
*                                              uC/MQTTc
*                                Message Queue Telemetry Transport Client
*
*                    Copyright 2014-2020 Silicon Laboratories Inc. www.silabs.com
*
*                                 SPDX-License-Identifier: APACHE-2.0
*
*               This software is subject to an open source license and is distributed by
*                Silicon Laboratories Inc. pursuant to the terms of the Apache License,
*                    Version 2.0 available at www.apache.org/licenses/LICENSE-2.0.
*
*********************************************************************************************************
*/

/*
*********************************************************************************************************
*
*                                             MQTT CLIENT
*
* Filename : mqtt-c.c
* Version  : V1.02.00
*********************************************************************************************************
*/

/*
*********************************************************************************************************
*********************************************************************************************************
*                                            INCLUDE FILES
*********************************************************************************************************
*********************************************************************************************************
*/

#define    MICRIUM_SOURCE
#define    MQTTc_MODULE

#include  <lib_def.h>
#include  <lib_str.h>
#include  <cpu.h>

#include  <mqtt-c_cfg.h>
#include  <KAL/kal.h>
#include  <Source/net_ascii.h>
#include  <Source/net_util.h>
#include  <Source/net_app.h>

#include  "mqtt-c.h"
#include  "mqtt-c_sock.h"
#include  "../../Common/mqtt.h"


/*
*********************************************************************************************************
*********************************************************************************************************
*                                            LOCAL DEFINES
*********************************************************************************************************
*********************************************************************************************************
*/

                                                                /* Offset in Rx publish buf to provide room to send ... */
                                                                /* a potential ACK without copying the whole buf.       */
#define MQTTc_PUBLISH_RX_MSG_BUF_OFFSET                             4u


/*
*********************************************************************************************************
*                                          DFLT VALUES DEFINES
*********************************************************************************************************
*/

#define  MQTTc_TIMEOUT_MS_DFLT_VAL                             10000u
#define  MQTTc_BROKER_PORT_NBR_DFLT_VAL                         1883u
#define  MQTTc_KEEP_ALIVE_TIMER_SEC_DFLT_VAL                       0u


/*
*********************************************************************************************************
*                                                  DBG
*********************************************************************************************************
*/

#if (MQTTc_CFG_DBG_GLOBAL_BUF_EN == DEF_ENABLED)
#define  MQTTc_DBG_GLOBAL_BUF_COPY(p_buf, len)       Mem_Copy(MQTTc_Dbg_GlobalBuf,  \
                                                             (p_buf),               \
                                                              DEF_MIN((len), MQTTc_CFG_DBG_GLOBAL_BUF_LEN))
#else
#define  MQTTc_DBG_GLOBAL_BUF_COPY(p_buf, len)
#endif


/*
*********************************************************************************************************
*********************************************************************************************************
*                                          LOCAL DATA TYPES
*********************************************************************************************************
*********************************************************************************************************
*/

typedef  struct  mqttc_data {
           MQTTc_CONN    *ConnHeadPtr;                          /* Ptr to head of conn list.                            */

           CPU_INT16U     MsgID_BitmapTblMax;                   /* Max msg ID.                                          */
    const  MQTTc_CFG     *CfgPtr;                               /* Ptr to cfg passed at init.                           */
           CPU_INT32U    *MsgID_BitmapTbl;                      /* Bitmap tbl for msg IDs.                              */
           MQTTc_MSG     *MsgListHeadPtr;                       /* Ptr to head of msg list to process.                  */
           MQTTc_MSG     *MsgListTailPtr;                       /* Ptr to tail of msg list to process.                  */
} MQTTc_DATA;


/*
*********************************************************************************************************
*********************************************************************************************************
*                                       LOCAL GLOBAL VARIABLES
*********************************************************************************************************
*********************************************************************************************************
*/

static  MQTTc_DATA  *MQTTc_Ptr = DEF_NULL;
#if (MQTTc_CFG_DBG_GLOBAL_BUF_EN == DEF_ENABLED)
        CPU_CHAR     MQTTc_Dbg_GlobalBuf[MQTTc_CFG_DBG_GLOBAL_BUF_LEN];
#endif


/*
*********************************************************************************************************
*********************************************************************************************************
*                                      LOCAL FUNCTION PROTOTYPES
*********************************************************************************************************
*********************************************************************************************************
*/

/*
*********************************************************************************************************
*                                   MSG AND TASK PROCESSING FUNCTIONS
*********************************************************************************************************
*/

static  void         MQTTc_Task                      (void            *p_arg);

static  void         MQTTc_WrSockProcess             (MQTTc_MSG       *p_msg);

static  void         MQTTc_RdSockProcess             (MQTTc_CONN      *p_conn);

static  void         MQTTc_MsgProcess                (void);

static  void         MQTTc_MsgCallbackExec           (MQTTc_MSG       *p_msg);


/*
*********************************************************************************************************
*                                            MSG Q FUNCTIONS
*********************************************************************************************************
*/

static  MQTTc_MSG   *MQTTc_MsgCheck                  (void);

static  void         MQTTc_MsgPost                   (MQTTc_CONN      *p_conn,
                                                      MQTTc_MSG       *p_msg,
                                                      MQTTc_MSG_TYPE   type,
                                                      CPU_INT32U       xfer_len,
                                                      CPU_INT08U       qos_lvl,
                                                      CPU_INT16U       msg_id,
                                                      MQTTc_ERR       *p_err);

static  void         MQTTc_MsgListClosedCallbackExec (MQTTc_MSG       *p_head_msg);


/*
*********************************************************************************************************
*                                             BUF FUNCTIONS
*********************************************************************************************************
*/

static  CPU_INT08U  *MQTTc_FixedHdrBufCfg            (CPU_INT08U      *p_buf,
                                                      MQTTc_MSG_TYPE   msg_type,
                                                      CPU_BOOLEAN      dup_flag,
                                                      CPU_INT08U       qos_lvl,
                                                      CPU_BOOLEAN      retain_flag,
                                                      CPU_INT32U       rem_len,
                                                      MQTTc_ERR       *p_err);


/*
*********************************************************************************************************
*                                           MSG ID FUNCTIONS
*********************************************************************************************************
*/

static  CPU_INT16U   MQTTc_MsgID_Get                 (void);

static  void         MQTTc_MsgID_Free                (CPU_INT16U       msg_id);


/*
*********************************************************************************************************
*                                            OTHER FUNCTIONS
*********************************************************************************************************
*/

static  void         MQTTc_ConnNextMsgClr            (MQTTc_CONN      *p_conn);

static  void         MQTTc_ConnCloseProc             (MQTTc_CONN      *p_conn,
                                                      MQTTc_ERR       *p_err);

static  void         MQTTc_ConnRemove                (MQTTc_CONN      *p_conn);


/*
*********************************************************************************************************
*********************************************************************************************************
*                                            GLOBAL FUNCTIONS
*********************************************************************************************************
*********************************************************************************************************
*/

/*
*********************************************************************************************************
*                                              MQTTc_Init()
*
* Description : Initializes the MQTTc module.
*
* Argument(s) : p_cfg           Pointer to MQTT Client Configuration Object.
*
*               p_task_cfg      Pointer to task configuration structure.
*
*               p_mem_seg       Memory segment from which internal data will be allocated. If DEF_NULL,
*                               will be allocated from the global heap.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to 'type'.
*                                   MQTTc_ERR_ALLOC             Failed to allocate data.
*                                   MQTTc_ERR_OS_FAIL           OS operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_Init (const  MQTTc_CFG     *p_cfg,
                  const  NET_TASK_CFG  *p_task_cfg,
                         MEM_SEG       *p_mem_seg,
                         MQTTc_ERR     *p_err)
{
    MQTTc_DATA      *p_temp_mqttc_data;
    KAL_TASK_HANDLE  task_handle;
    CPU_BOOLEAN      kal_feat_is_ok;
    KAL_ERR          err_kal;
    LIB_ERR          err_lib;
    CPU_SR_ALLOC();


                                                                /* --------------- ARGUMENTS VALIDATION --------------- */
    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_cfg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_cfg->MaxMsgNbr == 0u) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_task_cfg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
    #endif

    if (MQTTc_Ptr != DEF_NULL) {                                /* Make sure MQTTc module is not already init.          */
       *p_err = MQTTc_ERR_NONE;
        return;
    }

    KAL_Init(DEF_NULL,
            &err_kal);
    if (err_kal != KAL_ERR_NONE) {
       *p_err = MQTTc_ERR_OS_FAIL;
        return;
    }

    kal_feat_is_ok  = KAL_FeatureQuery(KAL_FEATURE_TASK_CREATE, KAL_OPT_CREATE_NONE);
    kal_feat_is_ok  = KAL_FeatureQuery(KAL_FEATURE_SEM_CREATE,  KAL_OPT_CREATE_NONE);
    kal_feat_is_ok  = KAL_FeatureQuery(KAL_FEATURE_SEM_PEND,    KAL_OPT_PEND_NONE);
    kal_feat_is_ok  = KAL_FeatureQuery(KAL_FEATURE_SEM_POST,    KAL_OPT_POST_NONE);
    kal_feat_is_ok  = KAL_FeatureQuery(KAL_FEATURE_SEM_DEL,     KAL_OPT_DEL_NONE);
    kal_feat_is_ok &= KAL_FeatureQuery(KAL_FEATURE_DLY,         KAL_OPT_DLY_NONE);
    if (kal_feat_is_ok != DEF_OK) {
       *p_err = MQTTc_ERR_OS_FAIL;
        return;
    }

                                                                /* Allocate data needed by MQTTc.                       */
    p_temp_mqttc_data = (MQTTc_DATA *)Mem_SegAlloc("MQTTc - Data",
                                                    p_mem_seg,
                                                    sizeof(MQTTc_DATA),
                                                   &err_lib);
    if (err_lib != LIB_MEM_ERR_NONE) {
       *p_err = MQTTc_ERR_ALLOC;
        return;
    }

    p_temp_mqttc_data->ConnHeadPtr = DEF_NULL;
    p_temp_mqttc_data->CfgPtr      = p_cfg;

                                                                /* Allocate msg ID bitmap tbl and calculate max ix.     */
    p_temp_mqttc_data->MsgID_BitmapTblMax = (p_cfg->MaxMsgNbr + (DEF_INT_32_NBR_BITS - 1u)) / DEF_INT_32_NBR_BITS;
    p_temp_mqttc_data->MsgID_BitmapTbl    = (CPU_INT32U *)Mem_SegAlloc("MQTTc - Msg ID Bitmap Tbl",
                                                                        p_mem_seg,
                                                                        sizeof(CPU_INT32U) * p_temp_mqttc_data->MsgID_BitmapTblMax,
                                                                       &err_lib);
    if (err_lib != LIB_MEM_ERR_NONE) {
       *p_err = MQTTc_ERR_ALLOC;
        return;
    }

    p_temp_mqttc_data->MsgListHeadPtr = DEF_NULL;               /* Init head of msg list.                               */
    p_temp_mqttc_data->MsgListTailPtr = DEF_NULL;               /* Init tail of msg list.                               */

                                                                /* Create task.                                         */
    task_handle = KAL_TaskAlloc("MQTTc Task",
                                 p_task_cfg->StkPtr,
                                 p_task_cfg->StkSizeBytes,
                                 DEF_NULL,
                                &err_kal);
    if (err_kal != KAL_ERR_NONE) {
       *p_err = MQTTc_ERR_OS_FAIL;
        return;
    }

    KAL_TaskCreate(task_handle,
                   MQTTc_Task,
                   DEF_NULL,
                   p_task_cfg->Prio,
                   DEF_NULL,
                  &err_kal);
    if (err_kal != KAL_ERR_NONE) {
       *p_err = MQTTc_ERR_OS_FAIL;
        return;
    }

    CPU_CRITICAL_ENTER();
    MQTTc_Ptr = p_temp_mqttc_data;
    CPU_CRITICAL_EXIT();

   *p_err = MQTTc_ERR_NONE;

    return;
}


/*
*********************************************************************************************************
*                                           MQTTc_ConnClr()
*
* Description : Clear an MQTTc Connection before the first usage.
*
* Argument(s) : p_conn          Pointer to the MQTTc Connection.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*
* Return(s)   : none.
*
* Caller(s)   : Application,
*               MQTTc_ConnRemove().
*
* Note(s)     : (1) This function MUST be called before the MQTTc_CONN object is used for the first time.
*********************************************************************************************************
*/

void  MQTTc_ConnClr (MQTTc_CONN  *p_conn,
                     MQTTc_ERR   *p_err)
{
                                                                /* --------------- ARGUMENTS VALIDATION --------------- */
    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
    {
        MQTTc_CONN  *p_conn_temp;


        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (MQTTc_Ptr == DEF_NULL) {                            /* Make sure MQTTc module is init.                      */
           *p_err = MQTTc_ERR_NOT_INIT;
            return;
        }
        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
                                                                /* Check if the Conn Obj is already used.               */
        p_conn_temp = MQTTc_Ptr->ConnHeadPtr;
        while (p_conn_temp != DEF_NULL) {
            if (p_conn_temp == p_conn) {
               *p_err = MQTTc_ERR_INVALID_ARG;
                return;
            }
            p_conn_temp = p_conn_temp->NextPtr;
        }
    }
    #endif

    p_conn->SockId              = NET_SOCK_ID_NONE;
    p_conn->SockSelFlags        = DEF_BIT_NONE;

    p_conn->BrokerNamePtr       = DEF_NULL;
    p_conn->BrokerPortNbr       = MQTTc_BROKER_PORT_NBR_DFLT_VAL;
    p_conn->InactivityTimeout_s = MQTTc_Ptr->CfgPtr->InactivityTimeout_s;

    p_conn->ClientID_Str        = DEF_NULL;
    p_conn->UsernameStr         = DEF_NULL;
    p_conn->PasswordStr         = DEF_NULL;

    p_conn->KeepAliveTimerSec   = MQTTc_KEEP_ALIVE_TIMER_SEC_DFLT_VAL;
    p_conn->WillCfgPtr          = DEF_NULL;

    p_conn->SecureCfgPtr        = DEF_NULL;

    p_conn->OnCmpl              = DEF_NULL;
    p_conn->OnConnectCmpl       = DEF_NULL;
    p_conn->OnPublishCmpl       = DEF_NULL;
    p_conn->OnSubscribeCmpl     = DEF_NULL;
    p_conn->OnUnsubscribeCmpl   = DEF_NULL;
    p_conn->OnPingReqCmpl       = DEF_NULL;
    p_conn->OnDisconnectCmpl    = DEF_NULL;
    p_conn->OnErrCallback       = DEF_NULL;
    p_conn->OnPublishRx         = DEF_NULL;
    p_conn->ArgPtr              = DEF_NULL;

    p_conn->TimeoutMs           = MQTTc_TIMEOUT_MS_DFLT_VAL;

    p_conn->PublishRxMsgPtr     = DEF_NULL;

    p_conn->TxMsgHeadPtr        = DEF_NULL;
    p_conn->NextTxMsgTxLen      = 0u;

    p_conn->NextPtr             = DEF_NULL;

    MQTTc_ConnNextMsgClr(p_conn);                               /* Clr all the NextMsg fields.                          */

   *p_err = MQTTc_ERR_NONE;

    return;
}


/*
*********************************************************************************************************
*                                         MQTTc_ConnSetParam()
*
* Description : Set parameters related to the TCP and MQTT Client Connection.
*
* Argument(s) : p_conn          Pointer to the current MQTTc Connection.
*
*               type            Parameter type :
*                                   MQTTc_PARAM_TYPE_BROKER_IP_ADDR                 Broker's IP addr.
*                                   MQTTc_PARAM_TYPE_BROKER_NAME                    Broker's name.
*                                   MQTTc_PARAM_TYPE_BROKER_PORT_NBR                Broker's port nbr.
*                                   MQTTc_PARAM_TYPE_INACTIVITY_TIMEOUT_S           Inactivity timeout, in seconds.
*                                   MQTTc_PARAM_TYPE_CLIENT_ID_STR                  Client ID str.
*                                   MQTTc_PARAM_TYPE_USERNAME_STR                   Client username str.
*                                   MQTTc_PARAM_TYPE_PASSWORD_STR                   Client password str.
*                                   MQTTc_PARAM_TYPE_KEEP_ALIVE_TMR_SEC             Keep alive tmr, in seconds.
*                                   MQTTc_PARAM_TYPE_WILL_CFG_PTR                   Will cfg ptr, if any.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ON_COMPL              Generic on     cmpl callback.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ON_CONNECT_CMPL       On connect     cmpl callback.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ON_PUBLISH_CMPL       On publish     cmpl callback.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ON_SUBSCRIBE_CMPL     On subscribe   cmpl callback.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ON_UNSUBSCRIBE_CMPL   On unsubscribe cmpl callback.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ON_PINGREQ_CMPL       On pingreq     cmpl callback.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ON_DISCONNECT_CMPL    On disconnect  cmpl callback.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ON_PUBLISH_RX         On publish rx'd callback.
*                                   MQTTc_PARAM_TYPE_CALLBACK_ARG_PTR               Ptr on arg passed to callback.
*                                   MQTTc_PARAM_TYPE_TIMEOUT_MS                     'Open' timeout, in milliseconds.
*                                   MQTTc_PARAM_TYPE_PUBLISH_RX_MSG_PTR             Ptr on msg that is used to rx publish.
*
*               p_param         Parameter's value.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to 'type'.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_ConnSetParam (MQTTc_CONN        *p_conn,
                          MQTTc_PARAM_TYPE   type,
                          void              *p_param,
                          MQTTc_ERR         *p_err)
{
                                                                /* --------------- ARGUMENTS VALIDATION --------------- */
    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_param == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
    #endif

    switch (type) {
        case MQTTc_PARAM_TYPE_BROKER_IP_ADDR:
        case MQTTc_PARAM_TYPE_BROKER_NAME:
             p_conn->BrokerNamePtr = (CPU_CHAR *)p_param;
             break;


        case MQTTc_PARAM_TYPE_BROKER_PORT_NBR:
             p_conn->BrokerPortNbr = (CPU_INT16U)(CPU_INT32U)p_param;
             break;


        case MQTTc_PARAM_TYPE_INACTIVITY_TIMEOUT_S:
             p_conn->InactivityTimeout_s = (CPU_INT16U)(CPU_INT32U)p_param;
             break;


        case MQTTc_PARAM_TYPE_CLIENT_ID_STR:
             p_conn->ClientID_Str = (CPU_CHAR *)p_param;
             break;


        case MQTTc_PARAM_TYPE_USERNAME_STR:
             p_conn->UsernameStr = (CPU_CHAR *)p_param;
             break;


        case MQTTc_PARAM_TYPE_PASSWORD_STR:
             p_conn->PasswordStr = (CPU_CHAR *)p_param;
             break;


        case MQTTc_PARAM_TYPE_KEEP_ALIVE_TMR_SEC:
             p_conn->KeepAliveTimerSec = (CPU_INT16U)(CPU_INT32U)p_param;
             break;


        case MQTTc_PARAM_TYPE_WILL_CFG_PTR:
             p_conn->WillCfgPtr = (MQTTc_WILL_CFG *)p_param;
             break;


        case MQTTc_PARAM_TYPE_SECURE_CFG_PTR:
             p_conn->SecureCfgPtr = (NET_APP_SOCK_SECURE_CFG *)p_param;
             break;


        case MQTTc_PARAM_TYPE_CALLBACK_ON_COMPL:
             p_conn->OnCmpl = (MQTTc_CMPL_CALLBACK)p_param;
             break;


        case MQTTc_PARAM_TYPE_CALLBACK_ON_CONNECT_CMPL:
             p_conn->OnConnectCmpl = (MQTTc_CMPL_CALLBACK)p_param;
             break;


        case MQTTc_PARAM_TYPE_CALLBACK_ON_PUBLISH_CMPL:
            p_conn->OnPublishCmpl = (MQTTc_CMPL_CALLBACK)p_param;
            break;


        case MQTTc_PARAM_TYPE_CALLBACK_ON_SUBSCRIBE_CMPL:
            p_conn->OnSubscribeCmpl = (MQTTc_CMPL_CALLBACK)p_param;
            break;


        case MQTTc_PARAM_TYPE_CALLBACK_ON_UNSUBSCRIBE_CMPL:
            p_conn->OnUnsubscribeCmpl = (MQTTc_CMPL_CALLBACK)p_param;
            break;


        case MQTTc_PARAM_TYPE_CALLBACK_ON_PINGREQ_CMPL:
            p_conn->OnPingReqCmpl = (MQTTc_CMPL_CALLBACK)p_param;
            break;


        case MQTTc_PARAM_TYPE_CALLBACK_ON_DISCONNECT_CMPL:
            p_conn->OnDisconnectCmpl = (MQTTc_CMPL_CALLBACK)p_param;
            break;

        case MQTTc_PARAM_TYPE_CALLBACK_ON_ERR_CALLBACK:
            p_conn->OnErrCallback = (MQTTc_ERR_CALLBACK)p_param;
            break;


        case MQTTc_PARAM_TYPE_CALLBACK_ON_PUBLISH_RX:
            p_conn->OnPublishRx = (MQTTc_PUBLISH_RX_CALLBACK)p_param;
            break;


        case MQTTc_PARAM_TYPE_CALLBACK_ARG_PTR:
             p_conn->ArgPtr = p_param;
             break;


        case MQTTc_PARAM_TYPE_TIMEOUT_MS:
             p_conn->TimeoutMs = (CPU_INT32U)p_param;
             break;


        case MQTTc_PARAM_TYPE_PUBLISH_RX_MSG_PTR:               /* Init msg to be use as RX publish msg.                */
             p_conn->PublishRxMsgPtr          = (MQTTc_MSG *)p_param;
             p_conn->PublishRxMsgPtr->ConnPtr =  p_conn;
             p_conn->PublishRxMsgPtr->Type    =  MQTTc_MSG_TYPE_PUBLISH;
             p_conn->PublishRxMsgPtr->State   =  MQTTc_MSG_STATE_WAIT_RX;
             p_conn->PublishRxMsgPtr->Err     =  MQTTc_ERR_NONE;
             p_conn->PublishRxMsgPtr->NextPtr =  DEF_NULL;
             break;


        default:
            *p_err = MQTTc_ERR_INVALID_ARG;
             return;
    }

   *p_err = MQTTc_ERR_NONE;

    return;
}


/*
*********************************************************************************************************
*                                            MQTTc_ConnOpen()
*
* Description : Open a new MQTT Client connection.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to open.
*
*               flags           Configuration flags, reserved for future usage.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_ConnOpen (MQTTc_CONN     *p_conn,
                      MQTTc_FLAGS     flags,
                      MQTTc_ERR      *p_err)
{
    (void)&flags;

                                                                /* --------------- ARGUMENTS VALIDATION --------------- */
    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
    #endif

    MQTTc_SockConnOpen(p_conn,
                       p_err);

    p_conn->TxMsgHeadPtr = DEF_NULL;
    p_conn->NextPtr      = DEF_NULL;

    return;
}


/*
*********************************************************************************************************
*                                          MQTTc_ConnClose()
*
* Description : Request a close for a MQTTc Connection.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection to close.
*
*               flags           Configuration flags, reserved for future usage.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Passed argument is invalid.
*                                   MQTTc_ERR_OS_FAIL           Problem occurred with signaling, in the OS.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : (1) A local message can be used only because the buffer is not referenced, in the case of
*                   a close request message. If the close request needs a valid buffer, the message will
*                   need to be allocated and configured by the caller of this funtion.
*********************************************************************************************************
*/

void  MQTTc_ConnClose (MQTTc_CONN   *p_conn,
                       MQTTc_FLAGS   flags,
                       MQTTc_ERR    *p_err)
{
    KAL_SEM_HANDLE  sem_handle;
    MQTTc_MSG       local_mqtt_msg;                             /* See Note #1.                                         */
    KAL_ERR         err_kal;


    (void)&flags;

                                                                /* --------------- ARGUMENTS VALIDATION --------------- */
    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_conn->SockId == NET_SOCK_ID_NONE) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }
    #endif

    sem_handle = KAL_SemCreate("MQTTc Close Sem",
                                DEF_NULL,
                               &err_kal);
    if (err_kal != KAL_ERR_NONE) {
       *p_err = MQTTc_ERR_ALLOC;
        return;
    }

    local_mqtt_msg.ArgPtr = (void *)&sem_handle;                /* Pass sem instead of buf, since it's a close req.     */

    MQTTc_MsgPost(p_conn,
                 &local_mqtt_msg,
                  MQTTc_MSG_TYPE_REQ_CLOSE,                     /* Indicate this msg is used to req a close.            */
                  0u,
                  0u,
                  MQTT_MSG_ID_NONE,
                  p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        goto end_err;                                           /* Do not pend if there was an err in msg posting.      */
    }

    KAL_SemPend(sem_handle,
                KAL_OPT_PEND_NONE,
                KAL_TIMEOUT_INFINITE,
               &err_kal);
    if (err_kal != KAL_ERR_NONE) {
       *p_err = MQTTc_ERR_OS_FAIL;
    } else {
       *p_err = local_mqtt_msg.Err;
    }

end_err:
    KAL_SemDel(sem_handle,
               &err_kal);
    (void)err_kal;

    return;
}


/*
*********************************************************************************************************
*                                            MQTTc_MsgClr()
*
* Description : Clear Message object members.
*
* Argument(s) : p_msg           Pointer to message object to clear.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_MsgClr (MQTTc_MSG  *p_msg,
                    MQTTc_ERR  *p_err)
{
                                                                /* --------------- ARGUMENTS VALIDATION --------------- */
    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_msg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
    #endif

    p_msg->ConnPtr = DEF_NULL;

    p_msg->Type    = MQTTc_MSG_TYPE_NONE;
    p_msg->State   = MQTTc_MSG_STATE_NONE;

    p_msg->QoS     = 0u;

    p_msg->MsgID   = MQTT_MSG_ID_NONE;

    p_msg->ArgPtr  = DEF_NULL;
    p_msg->BufLen  = 0u;
    p_msg->XferLen = 0u;

    p_msg->Err     = MQTTc_ERR_NONE;

    p_msg->NextPtr = DEF_NULL;

   *p_err = MQTTc_ERR_NONE;

    return;
}


/*
*********************************************************************************************************
*                                          MQTTc_MsgSetParam()
*
* Description : Set parameter related to a given MQTT Message.
*
* Argument(s) : p_msg           Pointer to message object.
*
*               type            Parameter type :
*                                   MQTTc_PARAM_TYPE_MSG_BUF_PTR        Msg's buf ptr.
*                                   MQTTc_PARAM_TYPE_MSG_BUF_LEN        Msg's buf len.
*
*               p_param         Parameter's value.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to 'type'.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_MsgSetParam (MQTTc_MSG         *p_msg,
                         MQTTc_PARAM_TYPE   type,
                         void              *p_param,
                         MQTTc_ERR         *p_err)
{
                                                                /* --------------- ARGUMENTS VALIDATION --------------- */
    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_msg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_param == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
    #endif

    switch (type) {
        case MQTTc_PARAM_TYPE_MSG_BUF_PTR:
             p_msg->ArgPtr = (void *)p_param;
             break;


        case MQTTc_PARAM_TYPE_MSG_BUF_LEN:
             p_msg->BufLen = (CPU_INT32U)p_param;
             break;


        default:
            *p_err = MQTTc_ERR_INVALID_ARG;
             return;
    }

   *p_err = MQTTc_ERR_NONE;

    return;
}


/*
*********************************************************************************************************
*                                            MQTTc_Connect()
*
* Description : Send a 'Connect' message to MQTT server.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection to use.
*
*               p_msg           Pointer to MQTTc Message object to use.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*                                   MQTTc_ERR_INVALID_BUF_SIZE  Invalid buf size passed to function.
*                                   MQTTc_ERR_FAIL              Operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_Connect (MQTTc_CONN  *p_conn,
                     MQTTc_MSG   *p_msg,
                     MQTTc_ERR   *p_err)
{
    MQTTc_WILL_CFG  *p_will_cfg;
    CPU_INT08U      *p_buf_start;
    CPU_INT08U      *p_buf;
    CPU_INT32U       xfer_len;
    CPU_INT32U       rem_len;
    CPU_INT16U       str_len;
    CPU_INT08U       conn_flags = 0u;


                                                                /* --------------- ARGUMENTS VALIDATION --------------- */
    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
    {
        CPU_INT08U  client_id_len;


        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (MQTTc_Ptr == DEF_NULL) {                            /* Make sure MQTTc module is init.                      */
           *p_err = MQTTc_ERR_NOT_INIT;
            return;
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_conn->PublishRxMsgPtr == DEF_NULL) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_conn->SockId == NET_SOCK_ID_NONE) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_conn->ClientID_Str == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        client_id_len = Str_Len(p_conn->ClientID_Str);          /* Make sure client ID is within spec limit.            */
        if (client_id_len > MQTT_MSG_VAR_HDR_CONNECT_CLIENT_ID_MAX_STR_LEN) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_conn->WillCfgPtr != DEF_NULL) {                   /* Confirm will cfg contains what is needed.            */
            if ((p_conn->WillCfgPtr->WillMessage == DEF_NULL) ||
                (p_conn->WillCfgPtr->WillTopic   == DEF_NULL)) {
               *p_err = MQTTc_ERR_NULL_PTR;
                return;
            }
        }

        if (p_msg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->ArgPtr == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->BufLen < MQTT_MSG_BASE_LEN) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }
    }
    #endif

    if (p_conn->PublishRxMsgPtr == DEF_NULL) {
       *p_err = MQTTc_ERR_NULL_PTR;
        return;
    }

    p_will_cfg = p_conn->WillCfgPtr;

    p_buf_start = (CPU_INT08U *)p_msg->ArgPtr;

    rem_len  = MQTT_MSG_VAR_HDR_CONNECT_LEN;                    /* Calculate len of msg.                                */
    rem_len += Str_Len(p_conn->ClientID_Str) + MQTT_MSG_UTF8_LEN_SIZE;
    if (p_will_cfg != DEF_NULL) {
        rem_len += Str_Len(p_will_cfg->WillTopic)   + MQTT_MSG_UTF8_LEN_SIZE;
        rem_len += Str_Len(p_will_cfg->WillMessage) + MQTT_MSG_UTF8_LEN_SIZE;
    }
    if (p_conn->UsernameStr != DEF_NULL) {
        rem_len += Str_Len(p_conn->UsernameStr) + MQTT_MSG_UTF8_LEN_SIZE;
    }
    if (p_conn->PasswordStr != DEF_NULL) {
        rem_len += Str_Len(p_conn->PasswordStr) + MQTT_MSG_UTF8_LEN_SIZE;
    }

    p_buf = MQTTc_FixedHdrBufCfg(p_buf_start,                   /* Cfg fixed hdr section of msg.                        */
                                 MQTTc_MSG_TYPE_CONNECT,
                                 DEF_NO,
                                 0u,
                                 DEF_NO,
                                 rem_len,
                                 p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        return;
    }

    if ((rem_len + (p_buf - p_buf_start)) > p_msg->BufLen) {    /* Confirm that buf can hold msg len.                   */
       *p_err = MQTTc_ERR_INVALID_BUF_SIZE;
        return;
    }

   *p_buf = 0x00;
    p_buf++;
   *p_buf = MQTT_MSG_VAR_HDR_PROTOCOL_NAME_LEN;
    p_buf++;

    Str_Copy_N((CPU_CHAR *)p_buf, MQTT_MSG_VAR_HDR_PROTOCOL_NAME_STR, MQTT_MSG_VAR_HDR_PROTOCOL_NAME_LEN);
    p_buf += MQTT_MSG_VAR_HDR_PROTOCOL_NAME_LEN;

   *p_buf = MQTT_MSG_VAR_HDR_PROTOCOL_VERSION;
    p_buf++;
                                                                /* Set CONNECT msg flags.                               */
    if (p_conn->UsernameStr != DEF_NULL) {
        DEF_BIT_SET(conn_flags, MQTT_MSG_VAR_HDR_CONNECT_FLAG_USER_NAME_FLAG);
    }

    if (p_conn->PasswordStr != DEF_NULL) {
        DEF_BIT_SET(conn_flags, MQTT_MSG_VAR_HDR_CONNECT_FLAG_PSWD_FLAG);
    }

    if (p_will_cfg != DEF_NULL) {
        DEF_BIT_SET(conn_flags, MQTT_MSG_VAR_HDR_CONNECT_FLAG_WILL_FLAG);
        DEF_BIT_SET(conn_flags, p_will_cfg->WillQoS << MQTT_MSG_VAR_HDR_CONNECT_FLAG_WILL_QOS_BIT_SHIFT);
        if (p_will_cfg->WillRetain == DEF_YES) {
            DEF_BIT_SET(conn_flags, MQTT_MSG_VAR_HDR_CONNECT_FLAG_WILL_RETAIN);
        }
    }

    DEF_BIT_SET(conn_flags, MQTT_MSG_VAR_HDR_CONNECT_FLAG_CLEAN_SESSION);

   *p_buf = conn_flags;
    p_buf++;

   *p_buf = (CPU_INT08U)(p_conn->KeepAliveTimerSec >> 8u);
    p_buf++;
   *p_buf = (CPU_INT08U)(p_conn->KeepAliveTimerSec & 0xFFu);
    p_buf++;

    str_len = Str_Len(p_conn->ClientID_Str);                    /* Copy client ID str.                                  */
   *p_buf = (CPU_INT08U)(str_len >> 8u);
    p_buf++;
   *p_buf = (CPU_INT08U)(str_len & 0xFFu);
    p_buf++;
    Str_Copy((CPU_CHAR *)p_buf, p_conn->ClientID_Str);
    p_buf += str_len;

    if (p_will_cfg != DEF_NULL) {                               /* Copy will infos, if any.                             */
        str_len = Str_Len(p_will_cfg->WillTopic);
       *p_buf = (CPU_INT08U)(str_len >> 8u);
        p_buf++;
       *p_buf = (CPU_INT08U)(str_len & 0xFFu);
        p_buf++;
        Str_Copy((CPU_CHAR *)p_buf, p_will_cfg->WillTopic);
        p_buf += str_len;

        str_len = Str_Len(p_will_cfg->WillMessage);
       *p_buf = (CPU_INT08U)(str_len >> 8u);
        p_buf++;
       *p_buf = (CPU_INT08U)(str_len & 0xFFu);
        p_buf++;
        Str_Copy((CPU_CHAR *)p_buf, p_will_cfg->WillMessage);
        p_buf += str_len;
    }

    if (p_conn->UsernameStr != DEF_NULL) {                      /* Copy username str, if any.                           */
        str_len = Str_Len(p_conn->UsernameStr);
       *p_buf = (CPU_INT08U)(str_len >> 8u);
        p_buf++;
       *p_buf = (CPU_INT08U)(str_len & 0xFFu);
        p_buf++;
        Str_Copy((CPU_CHAR *)p_buf, p_conn->UsernameStr);
        p_buf += str_len;
    }

    if (p_conn->PasswordStr != DEF_NULL) {                      /* Copy password str, if any.                           */
        str_len = Str_Len(p_conn->PasswordStr);
       *p_buf = (CPU_INT08U)(str_len >> 8u);
        p_buf++;
       *p_buf = (CPU_INT08U)(str_len & 0xFFu);
        p_buf++;
        Str_Copy((CPU_CHAR *)p_buf, p_conn->PasswordStr);
        p_buf += str_len;
    }

    MQTTc_DBG_GLOBAL_BUF_COPY(p_buf_start, 150u);

    xfer_len = p_buf - p_buf_start;

    MQTTc_MsgPost(p_conn,                                       /* Add msg to Q for task to process.                    */
                  p_msg,
                  MQTTc_MSG_TYPE_CONNECT,
                  xfer_len,
                  0u,
                  MQTT_MSG_ID_NONE,
                  p_err);

    return;
}


/*
*********************************************************************************************************
*                                            MQTTc_Publish()
*
* Description : Send a 'Publish' message to MQTT server.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to use.
*
*               p_msg           Pointer to MQTTc Message object to use.
*
*               topic_str       String containing the topic on which to publish.  Must stay valid until
*                               the message has been completely sent.
*
*               qos_lvl         Level of QoS at which to publish.
*
*               retain_flag     Flag indicating if the retain flag in the PUBLISH header needs to be set.
*
*               p_payload       Pointer to the payload to publish.  Must stay valid until the message has
*                               been completely sent.
*
*               payload_len     The length of the payload to publish.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*                                   MQTTc_ERR_INVALID_BUF_SIZE  Invalid buf size passed to function.
*                                   MQTTc_ERR_FAIL              Operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_Publish (       MQTTc_CONN    *p_conn,
                            MQTTc_MSG     *p_msg,
                     const  CPU_CHAR      *topic_str,
                            CPU_INT08U     qos_lvl,
                            CPU_BOOLEAN    retain_flag,
                     const  CPU_CHAR      *p_payload,
                            CPU_INT32U     payload_len,
                            MQTTc_ERR     *p_err)
{
    CPU_INT08U  *p_buf_start;
    CPU_INT08U  *p_buf;
    CPU_INT32U   xfer_len;
    CPU_INT32U   rem_len;
    CPU_INT16U   str_len;
    CPU_INT16U   msg_id      = MQTT_MSG_ID_NONE;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (MQTTc_Ptr == DEF_NULL) {                            /* Make sure MQTTc module is init.                      */
           *p_err = MQTTc_ERR_NOT_INIT;
            return;
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_conn->SockId == NET_SOCK_ID_NONE) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_msg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->ArgPtr == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if ((qos_lvl       != 0u) &&                            /* Make sure buf can at leasy hold reply from server.   */
            (p_msg->BufLen <  MQTT_MSG_BASE_LEN)) {
           *p_err = MQTTc_ERR_INVALID_BUF_SIZE;
            return;
        }

        if (topic_str == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (qos_lvl > MQTT_MSG_QOS_LVL_MAX) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        str_len =  Str_Len(topic_str);
        p_buf   = (CPU_INT08U *)Str_Char_N(topic_str,           /* # sign not allowed in topic.                         */
                                           str_len,
                                           ASCII_CHAR_NUMBER_SIGN);
        if (p_buf != DEF_NULL) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        p_buf = (CPU_INT08U *)Str_Char_N(topic_str,             /* + sign not allowed in topic.                         */
                                         str_len,
                                         ASCII_CHAR_PLUS_SIGN);
        if (p_buf != DEF_NULL) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }
    #endif

    p_buf_start = (CPU_INT08U *)p_msg->ArgPtr;

    rem_len = Str_Len(topic_str) + MQTT_MSG_UTF8_LEN_SIZE;
    if (qos_lvl > 0u) {
        rem_len += MQTT_MSG_ID_SIZE;
    }
    rem_len += payload_len;

    p_buf = MQTTc_FixedHdrBufCfg(p_buf_start,                   /* Cfg fixed section of hdr.                            */
                                 MQTTc_MSG_TYPE_PUBLISH,
                                 DEF_NO,
                                 qos_lvl,
                                 retain_flag,
                                 rem_len,
                                 p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        return;
    }

    if ((rem_len + ((CPU_INT16U)(p_buf - p_buf_start))) > p_msg->BufLen) {
                                                                /* Confirm msg fits in provided buf.                    */
       *p_err = MQTTc_ERR_INVALID_BUF_SIZE;
        return;
    }

    str_len = Str_Len(topic_str);                               /* Copy topic str.                                      */
   *p_buf = (CPU_INT08U)(str_len >> 8u);
    p_buf++;
   *p_buf = (CPU_INT08U)(str_len & 0xFFu);
    p_buf++;
    Str_Copy((CPU_CHAR *)p_buf, topic_str);

    p_buf += str_len;

    if (qos_lvl > 0u) {                                         /* Obtain msg ID if QoS > 0.                            */
        msg_id = MQTTc_MsgID_Get();

       *p_buf = (CPU_INT08U)(msg_id >> 8u);
        p_buf++;
       *p_buf = (CPU_INT08U)(msg_id & 0xFFu);
        p_buf++;
    }

    Mem_Copy(p_buf,                                             /* Copy payload.                                        */
             p_payload,
             payload_len);

    p_buf += payload_len;

    xfer_len = p_buf - p_buf_start;

    MQTTc_DBG_GLOBAL_BUF_COPY(p_buf, 150u);

    MQTTc_MsgPost(p_conn,                                       /* Post msg to Q for task to process.                   */
                  p_msg,
                  MQTTc_MSG_TYPE_PUBLISH,
                  xfer_len,
                  qos_lvl,
                  msg_id,
                  p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        MQTTc_MsgID_Free(msg_id);
    }

    return;
}


/*
*********************************************************************************************************
*                                           MQTTc_Subscribe()
*
* Description : Send a 'Subscribe' message to MQTT server.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to use.
*
*               p_msg           Pointer to MQTTc Message object to use.
*
*               topic_str       String containing the topic at which to subscribe. Must stay valid until
*                               the message has been completely sent.
*
*               req_qos         Requested level of QoS for this subscription.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*                                   MQTTc_ERR_INVALID_BUF_SIZE  Invalid buf size passed to function.
*                                   MQTTc_ERR_FAIL              Operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_Subscribe (       MQTTc_CONN  *p_conn,
                              MQTTc_MSG   *p_msg,
                       const  CPU_CHAR    *topic_str,
                              CPU_INT08U   req_qos,
                              MQTTc_ERR   *p_err)
{
    MQTTc_SubscribeMult(p_conn,
                        p_msg,
                       &topic_str,
                       &req_qos,
                        1u,
                        p_err);
}


/*
*********************************************************************************************************
*                                         MQTTc_SubscribeMult()
*
* Description : Send a 'Subscribe' message containing multiple topics to MQTT server.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to use.
*
*               p_msg           Pointer to MQTTc Message object to use.
*
*               topic_str_tbl   Table containing string of all the topic(s) at which to subscribe. Must
*                               all stay valid until the message has been completely sent.
*
*               req_qos_tbl     Table of the requested level of QoS for each subscription.
*
*               topic_nbr       Number of topic and QoS contained in tables.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*                                   MQTTc_ERR_INVALID_BUF_SIZE  Invalid buf size passed to function.
*                                   MQTTc_ERR_FAIL              Operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : (1) To compare with the granted QoS returned by the server with the SUBACK message, the
*                   number of topics and their QoSes are kept before the actual content to send to the
*                   server. For 3 topics, the buffer would look like this:
*
*                   Original p_msg->BufPtr           Modified p_msg->BufPtr
*                                   |               |   Start of msg sent to server.
*                                   |               |  |
*                                   V               V  V
*                                   ------------------------------
*                                   | 2 | 0 | 1 | 3 | 0x82 | ... |
*                                   ------------------------------
*                                    ^    ^   ^   ^
*                                    |    |   |   |
*                          QoS Topic 3    |   |   |
*                               QoS Topic 2   |   |
*                                   QoS Topic 1   |
*                                          Topic Nbr
*********************************************************************************************************
*/

void  MQTTc_SubscribeMult (       MQTTc_CONN   *p_conn,
                                  MQTTc_MSG    *p_msg,
                           const  CPU_CHAR    **topic_str_tbl,
                                  CPU_INT08U   *req_qos_tbl,
                                  CPU_INT08U    topic_nbr,
                                  MQTTc_ERR    *p_err)
{
    CPU_INT08U  *p_buf_base;
    CPU_INT08U  *p_buf_start;
    CPU_INT08U  *p_buf;
    CPU_INT32U   xfer_len;
    CPU_INT16U   str_len;
    CPU_INT16U   rem_len;
    CPU_INT16U   msg_id;
    CPU_INT08U   topic_ix;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (MQTTc_Ptr == DEF_NULL) {                            /* Make sure MQTTc module is init.                      */
           *p_err = MQTTc_ERR_NOT_INIT;
            return;
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_conn->SockId == NET_SOCK_ID_NONE) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_msg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->ArgPtr == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

                                                                /* Make sure buf can hold topics of 1 byte and its QoS. */
        if (p_msg->BufLen < (MQTT_MSG_BASE_LEN + 1u + (topic_nbr * (MQTT_MSG_UTF8_LEN_SIZE + 1u)))) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (topic_str_tbl == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (req_qos_tbl == DEF_NULL) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }
    #endif

    p_buf_start = (CPU_INT08U *)p_msg->ArgPtr;
    p_buf_base  = (CPU_INT08U *)p_msg->ArgPtr;
    rem_len     =  MQTT_MSG_ID_SIZE;

    for (topic_ix = 0u; topic_ix < topic_nbr; topic_ix++) {     /* Calculate len of all topics and their QoS.           */
        #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
                                                                /* Make sure each ix contains a valid str and QoS.      */
            if (topic_str_tbl[topic_ix] == DEF_NULL) {
               *p_err = MQTTc_ERR_NULL_PTR;
                return;
            }

            if (req_qos_tbl[topic_ix] > MQTT_MSG_QOS_LVL_MAX) {
               *p_err = MQTTc_ERR_INVALID_ARG;
                return;
            }
        #endif

        str_len = Str_Len(topic_str_tbl[topic_ix]);

        #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
            p_buf = (CPU_INT08U *)Str_Char_N(topic_str_tbl[topic_ix],
                                             str_len,
                                             ASCII_CHAR_NUMBER_SIGN);
            if (p_buf != DEF_NULL) {
                if (str_len != 1u) {                            /* If topic is '#' by itself, no err.                   */
                    if (*(p_buf - 1u) != ASCII_CHAR_SOLIDUS) {  /* '#' must be preceded by a '/'.                       */
                       *p_err = MQTTc_ERR_INVALID_ARG;
                        return;
                    }
                                                                /* '#' must be the last character in topic.             */
                    if (p_buf != (CPU_INT08U *)&topic_str_tbl[topic_ix][str_len - 1u]) {
                       *p_err = MQTTc_ERR_INVALID_ARG;
                        return;
                    }
                }
            }

            p_buf = (CPU_INT08U *)Str_Char_N(topic_str_tbl[topic_ix],
                                             str_len,
                                             ASCII_CHAR_PLUS_SIGN);
            if (p_buf != DEF_NULL) {
                if (str_len != 1u) {                            /* If topic is '+' by itself, no err.                   */
                    if (*(p_buf - 1u) != ASCII_CHAR_SOLIDUS) {  /* '+' must be preceded by a '/'.                       */
                       *p_err = MQTTc_ERR_INVALID_ARG;
                        return;
                    }
                                                                /* If '+' is not last char, it must be followed by '/'. */
                    if ((  p_buf       != (CPU_INT08U *)&topic_str_tbl[topic_ix][str_len - 1u]) &&
                        (*(p_buf + 1u) !=  ASCII_CHAR_SOLIDUS)) {
                       *p_err = MQTTc_ERR_INVALID_ARG;
                        return;
                    }
                }
            }
        #endif

        rem_len += str_len + MQTT_MSG_UTF8_LEN_SIZE;
        rem_len += 1u;
    }

                                                                /* Write topics QoS and nbr of topics before ...        */
                                                                /* content. See Note #1.                                */
    for (topic_ix = topic_nbr; topic_ix > 0u; topic_ix--) {
       *p_buf_start = req_qos_tbl[topic_ix - 1u];
        p_buf_start++;
    }
   *p_buf_start = topic_nbr;
    p_buf_start++;

    MQTTc_DBG_GLOBAL_BUF_COPY(p_buf_base, 10u);

    p_buf = MQTTc_FixedHdrBufCfg(p_buf_start,                   /* Cfg fixed hdr section of msg.                        */
                                 MQTTc_MSG_TYPE_SUBSCRIBE,
                                 DEF_NO,
                                 1u,
                                 DEF_NO,
                                 rem_len,
                                 p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        return;
    }

    if ((rem_len + ((CPU_INT16U)(p_buf - p_buf_base))) > p_msg->BufLen) {
                                                                /* Confirm msg fits in provided buf.                    */
       *p_err = MQTTc_ERR_INVALID_BUF_SIZE;
        return;
    }

    msg_id = MQTTc_MsgID_Get();
   *p_buf = (CPU_INT08U)(msg_id >> 8u);
    p_buf++;
   *p_buf = (CPU_INT08U)(msg_id & 0xFFu);
    p_buf++;

    for (topic_ix = 0u; topic_ix < topic_nbr; topic_ix++) {     /* Calculate len of all topics and their QoS.           */

        str_len = Str_Len(topic_str_tbl[topic_ix]);

       *p_buf = (CPU_INT08U)(str_len >> 8u);
        p_buf++;
       *p_buf = (CPU_INT08U)(str_len & 0xFFu);
        p_buf++;

        Str_Copy((CPU_CHAR *)p_buf, topic_str_tbl[topic_ix]);

        p_buf += str_len;

       *p_buf = req_qos_tbl[topic_ix];
        p_buf++;
    }

    xfer_len = p_buf - p_buf_start;

    MQTTc_DBG_GLOBAL_BUF_COPY(p_buf_start, 150u);

    p_msg->ArgPtr  = (void *)p_buf_start;                       /* Adjust BufPtr to start of content to send.           */
    p_msg->BufLen -= (topic_nbr + 1u);                          /* Adjust BufLen to account for topics Qos.             */

    MQTTc_MsgPost(p_conn,                                       /* Post msg to Q for task to process.                   */
                  p_msg,
                  MQTTc_MSG_TYPE_SUBSCRIBE,
                  xfer_len,
                  1u,
                  msg_id,
                  p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        MQTTc_MsgID_Free(msg_id);
    }

    return;
}


/*
*********************************************************************************************************
*                                          MQTTc_Unsubscribe()
*
* Description : Send a 'Unsubscribe' message to MQTT server.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to use.
*
*               p_msg           Pointer to MQTTc Message object to use.
*
*               topic_str       String containing the topic at which to unsubscribe. Must stay valid
*                               until the message has been completely sent.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*                                   MQTTc_ERR_INVALID_BUF_SIZE  Invalid buf size passed to function.
*                                   MQTTc_ERR_FAIL              Operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_Unsubscribe (       MQTTc_CONN  *p_conn,
                                MQTTc_MSG   *p_msg,
                         const  CPU_CHAR    *topic_str,
                                MQTTc_ERR   *p_err)
{
    MQTTc_UnsubscribeMult(p_conn,
                          p_msg,
                         &topic_str,
                          1u,
                          p_err);
}


/*
*********************************************************************************************************
*                                        MQTTc_UnsubscribeMult()
*
* Description : Send a 'Unsubscribe' message for multiple topics to MQTT server.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to use.
*
*               p_msg           Pointer to MQTTc Message object to use.
*
*               topic_str_tbl   Table containing string of all the topic(s) at which to unsubscribe. Must
*                               all stay valid until the message has been completely sent.
*
*               topic_nbr       Number of topic contained in tables.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*                                   MQTTc_ERR_INVALID_BUF_SIZE  Invalid buf size passed to function.
*                                   MQTTc_ERR_FAIL              Operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_UnsubscribeMult (       MQTTc_CONN   *p_conn,
                                    MQTTc_MSG    *p_msg,
                             const  CPU_CHAR    **topic_str_tbl,
                                    CPU_INT08U    topic_nbr,
                                    MQTTc_ERR    *p_err)
{
    CPU_INT08U  *p_buf_start;
    CPU_INT08U  *p_buf;
    CPU_INT32U   xfer_len;
    CPU_INT16U   str_len;
    CPU_INT16U   rem_len;
    CPU_INT16U   msg_id;
    CPU_INT08U   topic_ix;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (MQTTc_Ptr == DEF_NULL) {                            /* Make sure MQTTc module is init.                      */
           *p_err = MQTTc_ERR_NOT_INIT;
            return;
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_conn->SockId == NET_SOCK_ID_NONE) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_msg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->ArgPtr == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->BufLen < MQTT_MSG_BASE_LEN) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (topic_str_tbl == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
    #endif

    p_buf_start = (CPU_INT08U *)p_msg->ArgPtr;
    rem_len     =  MQTT_MSG_ID_SIZE;

    for (topic_ix = 0u; topic_ix < topic_nbr; topic_ix++) {     /* Calculate len of all topics.                         */
        #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
            if (topic_str_tbl[topic_ix] == DEF_NULL) {          /* Make sure each ix contains a valid str.              */
               *p_err = MQTTc_ERR_NULL_PTR;
                return;
            }
        #endif

        str_len  = Str_Len(topic_str_tbl[topic_ix]);
        rem_len += str_len + MQTT_MSG_UTF8_LEN_SIZE;
    }

    p_buf = MQTTc_FixedHdrBufCfg(p_buf_start,                   /* Cfg fixed hdr section of msg.                        */
                                 MQTTc_MSG_TYPE_UNSUBSCRIBE,
                                 DEF_NO,
                                 1u,
                                 DEF_NO,
                                 rem_len,
                                 p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        return;
    }

    if ((rem_len + ((CPU_INT16U)(p_buf - p_buf_start))) > p_msg->BufLen) {
                                                                /* Confirm msg fits in provided buf.                    */
       *p_err = MQTTc_ERR_INVALID_BUF_SIZE;
        return;
    }

    msg_id = MQTTc_MsgID_Get();                                 /* Obtain msg ID.                                       */
   *p_buf = (CPU_INT08U)(msg_id >> 8u);
    p_buf++;
   *p_buf = (CPU_INT08U)(msg_id & 0xFFu);
    p_buf++;

    for (topic_ix = 0u; topic_ix < topic_nbr; topic_ix++) {
        str_len = Str_Len(topic_str_tbl[topic_ix]);

       *p_buf = (CPU_INT08U)(str_len >> 8u);
        p_buf++;
       *p_buf = (CPU_INT08U)(str_len & 0xFFu);
        p_buf++;

        Str_Copy((CPU_CHAR *)p_buf, topic_str_tbl[topic_ix]);

        p_buf += str_len;
    }

    xfer_len = p_buf - p_buf_start;

    MQTTc_DBG_GLOBAL_BUF_COPY(p_buf_start, 150u);

    MQTTc_MsgPost(p_conn,                                       /* Post msg in Q for task to process.                   */
                  p_msg,
                  MQTTc_MSG_TYPE_UNSUBSCRIBE,
                  xfer_len,
                  1u,
                  msg_id,
                  p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        MQTTc_MsgID_Free(msg_id);
    }

    return;
}


/*
*********************************************************************************************************
*                                            MQTTc_PingReq()
*
* Description : Send a 'PingReq' message to MQTT server.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to use.
*
*               p_msg           Pointer to MQTTc Message object to use.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*                                   MQTTc_ERR_FAIL              Operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_PingReq (MQTTc_CONN  *p_conn,
                     MQTTc_MSG   *p_msg,
                     MQTTc_ERR   *p_err)
{
    CPU_INT08U  *p_buf_start;
    CPU_INT08U  *p_buf;
    CPU_INT32U   xfer_len;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (MQTTc_Ptr == DEF_NULL) {                            /* Make sure MQTTc module is init.                      */
           *p_err = MQTTc_ERR_NOT_INIT;
            return;
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_conn->SockId == NET_SOCK_ID_NONE) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_msg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->ArgPtr == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->BufLen < MQTT_MSG_PING_DISCONN_LEN) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }
    #endif

    p_buf_start = (CPU_INT08U *)p_msg->ArgPtr;

    p_buf = MQTTc_FixedHdrBufCfg(p_buf_start,                   /* Cfg fixed hdr section of msg.                        */
                                 MQTTc_MSG_TYPE_PINGREQ,
                                 DEF_NO,
                                 0u,
                                 DEF_NO,
                                 0u,
                                 p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        return;
    }

    xfer_len = p_buf - p_buf_start;

    MQTTc_DBG_GLOBAL_BUF_COPY(p_buf, 150u);

    MQTTc_MsgPost(p_conn,                                       /* Post msg in Q for task to process.                   */
                  p_msg,
                  MQTTc_MSG_TYPE_PINGREQ,
                  xfer_len,
                  0u,
                  MQTT_MSG_ID_NONE,
                  p_err);

    return;
}


/*
*********************************************************************************************************
*                                          MQTTc_Disconnect()
*
* Description : Send a 'Disconnect' message to MQTT server.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to use.
*
*               p_msg           Pointer to MQTTc Message object to use.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NOT_INIT          MQTTc module has not yet been initialized.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*                                   MQTTc_ERR_FAIL              Operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_Disconnect (MQTTc_CONN  *p_conn,
                        MQTTc_MSG   *p_msg,
                        MQTTc_ERR   *p_err)
{
    CPU_INT08U  *p_buf;
    CPU_INT08U  *p_buf_start;
    CPU_INT32U   xfer_len;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (MQTTc_Ptr == DEF_NULL) {                            /* Make sure MQTTc module is init.                      */
           *p_err = MQTTc_ERR_NOT_INIT;
            return;
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_conn->SockId == NET_SOCK_ID_NONE) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }

        if (p_msg == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_msg->BufLen < MQTT_MSG_PING_DISCONN_LEN) {
           *p_err = MQTTc_ERR_INVALID_ARG;
            return;
        }
    #endif

    p_buf_start = (CPU_INT08U *)p_msg->ArgPtr;

    p_buf = MQTTc_FixedHdrBufCfg(p_buf_start,                   /* Cfg fixed hdr section of msg.                        */
                                 MQTTc_MSG_TYPE_DISCONNECT,
                                 DEF_NO,
                                 0u,
                                 DEF_NO,
                                 0u,
                                 p_err);
    if (*p_err != MQTTc_ERR_NONE) {
        return;
    }

    xfer_len = p_buf - p_buf_start;

    MQTTc_DBG_GLOBAL_BUF_COPY(p_buf, 150u);

    MQTTc_MsgPost(p_conn,                                       /* Post msg in Q for task to process.                   */
                  p_msg,
                  MQTTc_MSG_TYPE_DISCONNECT,
                  xfer_len,
                  0u,
                  MQTT_MSG_ID_NONE,
                  p_err);

    return;
}


/*
*********************************************************************************************************
*********************************************************************************************************
*                                           LOCAL FUNCTIONS
*********************************************************************************************************
*********************************************************************************************************
*/

/*
*********************************************************************************************************
*                                             MQTTc_Task()
*
* Description : Task for MQTTc. Process select, read, write and msg Q.
*
* Argument(s) : p_arg           Unused argument.
*
* Return(s)   : none.
*
* Caller(s)   : This is a task.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_Task (void  *p_arg)
{
    MQTTc_CONN    *p_conn;
    CPU_BOOLEAN    proc_rd;
    CPU_BOOLEAN    proc_wr;
    CPU_BOOLEAN    proc_err;
    CPU_BOOLEAN    is_init   = DEF_NO;
    CPU_INT32U     dly;
    MQTTc_ERR      err_mqttc;


    (void)&p_arg;

    while (is_init != DEF_YES) {                                /* Wait for MQTTc module to be init.                    */
        CPU_SR_ALLOC();


        KAL_Dly(1u);
        CPU_CRITICAL_ENTER();
        is_init = (MQTTc_Ptr != DEF_NULL) ? DEF_YES : DEF_NO;
        CPU_CRITICAL_EXIT();
    }

    while (DEF_TRUE) {

        if (MQTTc_Ptr->ConnHeadPtr != DEF_NULL) {
            dly = MQTTc_Ptr->CfgPtr->TaskDly;

            MQTTc_SockSel(MQTTc_Ptr->ConnHeadPtr,
                         &err_mqttc);

            if (err_mqttc == MQTTc_ERR_NONE) {

                p_conn = MQTTc_Ptr->ConnHeadPtr;

                while (p_conn != DEF_NULL) {
                    MQTTc_CONN  *p_conn_next = p_conn->NextPtr;


                    proc_rd  = MQTTc_SockSelDescProc(p_conn, MQTTc_SEL_DESC_TYPE_RD);
                    proc_wr  = MQTTc_SockSelDescProc(p_conn, MQTTc_SEL_DESC_TYPE_WR);
                    proc_err = MQTTc_SockSelDescProc(p_conn, MQTTc_SEL_DESC_TYPE_ERR);


                    if (proc_err == DEF_YES) {
                        MQTTc_ERR_CALLBACK   on_err_callback;
                        void                *p_callback_arg;


                        on_err_callback = p_conn->OnErrCallback;
                        p_callback_arg  = p_conn->ArgPtr;

                        MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! Sock sel error for sock ID %i. Closing it.\r\n", p_conn->SockId));

                        MQTTc_ConnCloseProc(p_conn,
                                           &err_mqttc);

                        if (on_err_callback != DEF_NULL) {
                            on_err_callback(p_conn,
                                            p_callback_arg,
                                            MQTTc_ERR_SOCK_FAIL);
                        }

                    } else if (proc_wr == DEF_YES) {
                        MQTTc_MSG  *p_msg = DEF_NULL;


                        if (p_conn->PublishRxMsgPtr->State == MQTTc_MSG_STATE_WAIT_TX_CMPL) {
                            p_msg = p_conn->PublishRxMsgPtr;
                        } else if ((p_conn->TxMsgHeadPtr        != DEF_NULL) &&
                                   (p_conn->TxMsgHeadPtr->State == MQTTc_MSG_STATE_WAIT_TX_CMPL)) {
                            p_msg = p_conn->TxMsgHeadPtr;
                        } else if (p_conn->PublishRxMsgPtr->State == MQTTc_MSG_STATE_MUST_TX) {
                            p_msg = p_conn->PublishRxMsgPtr;
                        } else if ((p_conn->TxMsgHeadPtr        != DEF_NULL) &&
                                   (p_conn->TxMsgHeadPtr->State == MQTTc_MSG_STATE_MUST_TX)) {
                            p_msg = p_conn->TxMsgHeadPtr;
                        } else {
                            MQTTc_SockSelDescClr(p_conn, MQTTc_SEL_DESC_TYPE_WR);
                        }

                        if (p_msg != DEF_NULL) {
                            MQTTc_WrSockProcess(p_msg);
                        }
                    } else if (proc_rd == DEF_YES) {
                        MQTTc_RdSockProcess(p_conn);
                    }

                    if ((p_conn->TxMsgHeadPtr        != DEF_NULL) &&
                        (p_conn->TxMsgHeadPtr->State == MQTTc_MSG_STATE_MUST_TX)) {
                        MQTTc_SockSelDescSet(p_conn, MQTTc_SEL_DESC_TYPE_WR);
                    }
                    p_conn = p_conn_next;
                }
            }
        } else {
            dly = DEF_MAX(1u, MQTTc_Ptr->CfgPtr->TaskDly);      /* In this case the task must absolutely dly.           */
        }

        MQTTc_MsgProcess();

        KAL_Dly(dly);
    }
}


/*
*********************************************************************************************************
*                                         MQTTc_WrSockProcess()
*
* Description : Process write operations required for given MQTTc message.
*
* Argument(s) : p_msg           Pointer to MQTTc Message object for which to process write operation.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_Task().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_WrSockProcess (MQTTc_MSG  *p_msg)
{
    MQTTc_CONN  *p_conn = p_msg->ConnPtr;
    CPU_INT32U   buf_len;


    if (p_msg->State == MQTTc_MSG_STATE_MUST_TX) {              /* If msg needs to be tx'd, tx it.                      */

        switch (p_msg->Type) {
            case MQTTc_MSG_TYPE_CONNECT:
                                                                /* Set Rd and Err sel desc, to be able to rx CONNACK.   */
                 MQTTc_SockSelDescSet(p_conn, MQTTc_SEL_DESC_TYPE_RD);
                 MQTTc_SockSelDescSet(p_conn, MQTTc_SEL_DESC_TYPE_ERR);
                                                                /* break intentionally omitted.                         */
            case MQTTc_MSG_TYPE_PUBLISH:
            case MQTTc_MSG_TYPE_PUBACK:
            case MQTTc_MSG_TYPE_PUBREC:
            case MQTTc_MSG_TYPE_PUBREL:
            case MQTTc_MSG_TYPE_PUBCOMP:
            case MQTTc_MSG_TYPE_SUBSCRIBE:
            case MQTTc_MSG_TYPE_UNSUBSCRIBE:
            case MQTTc_MSG_TYPE_PINGREQ:
            case MQTTc_MSG_TYPE_DISCONNECT:
                 buf_len = DEF_MIN((p_msg->XferLen - p_conn->NextTxMsgTxLen), DEF_INT_16U_MAX_VAL);
                 MQTTc_DBG_TRACE_DBG(("Transmitting %i bytes on sock ID %i. Msg Type: %i\r\n",
                                       p_msg->XferLen,
                                       p_conn->SockId,
                                       p_msg->Type));
                 p_conn->NextTxMsgTxLen += MQTTc_SockTx(   p_conn,
                                                       &(((CPU_INT08U *)p_msg->ArgPtr)[p_conn->NextTxMsgTxLen]),
													       buf_len,
                                                          &p_msg->Err);
                 if (p_msg->Err != MQTTc_ERR_NONE) {            /* If err, exec callback and return.                    */
                     MQTTc_MsgCallbackExec(p_msg);
                 }
                 if (p_conn->NextTxMsgTxLen == p_msg->XferLen) {
                     p_msg->State           = MQTTc_MSG_STATE_WAIT_TX_CMPL;
                     p_conn->NextTxMsgTxLen = 0u;
                 }
                 break;


            case MQTTc_MSG_TYPE_CONNACK:                        /* These cases should never happen.                     */
            case MQTTc_MSG_TYPE_SUBACK:
            case MQTTc_MSG_TYPE_UNSUBACK:
            case MQTTc_MSG_TYPE_PINGRESP:
            default:
                 MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! In must Tx switch default case.\n\r"));
                 break;
        }
    } else if (p_msg->State == MQTTc_MSG_STATE_WAIT_TX_CMPL) {
        CPU_INT08U  *p_buf_topic_nbr;
        CPU_INT08U   topic_nbr;


                                                                /* Tx operation has finished. Go to next step of msg.   */
        switch (p_msg->Type) {
            case MQTTc_MSG_TYPE_CONNECT:                        /* Finished sending a CONNECT, wait to rx CONNACK reply.*/
                 MQTTc_DBG_TRACE_LOG(("Finished sending Connect. Waiting to Rx Connack.\r\n"));

                 p_msg->Type    = MQTTc_MSG_TYPE_CONNACK;
                 p_msg->State   = MQTTc_MSG_STATE_WAIT_RX;
                 p_msg->XferLen = 2u;
                 break;


            case MQTTc_MSG_TYPE_PUBLISH:
                 if (p_msg->QoS == 0u) {                        /* If QoS is 0, xfer is cmpl.                           */
                     MQTTc_DBG_TRACE_LOG(("Finished sending Publish QoS 0. Executing callback.\r\n"));
                     p_msg->Err = MQTTc_ERR_NONE;
                     MQTTc_MsgCallbackExec(p_msg);
                 } else if (p_msg->QoS == 1u) {                 /* If QoS is 1, send PUBACK reply.                      */
                     MQTTc_DBG_TRACE_LOG(("Finished sending Publish QoS 1. Waiting to Rx Puback.\r\n"));
                     p_msg->Type    = MQTTc_MSG_TYPE_PUBACK;
                     p_msg->State   = MQTTc_MSG_STATE_WAIT_RX;
                     p_msg->XferLen = 0u;
                 } else {                                       /* If QoS is 2, send PUBREC reply.                      */
                     MQTTc_DBG_TRACE_LOG(("Finished sending Publish QoS 2. Waiting to Rx Pubrec.\r\n"));
                     p_msg->Type    = MQTTc_MSG_TYPE_PUBREC;
                     p_msg->State   = MQTTc_MSG_STATE_WAIT_RX;
                     p_msg->XferLen = 0u;
                 }
                 break;


            case MQTTc_MSG_TYPE_PUBACK:                         /* Finished sending a PUBACK, xfer is cmpl.             */
                 MQTTc_DBG_TRACE_LOG(("Finished sending a Puback. Removing msg from list.\r\n"));
                 p_msg->Type  = MQTTc_MSG_TYPE_PUBLISH;
                 p_msg->State = MQTTc_MSG_STATE_WAIT_RX;
                 p_msg->Err   = MQTTc_ERR_NONE;
                 break;


            case MQTTc_MSG_TYPE_PUBREC:                         /* Finished sending a PUBREC, wait to rx PUBREL.        */
                 MQTTc_DBG_TRACE_LOG(("Finished sending a Pubrec. Waiting to Rx a Pubrel.\r\n"));
                 p_msg->Type    = MQTTc_MSG_TYPE_PUBREL;
                 p_msg->State   = MQTTc_MSG_STATE_WAIT_RX;
                 p_msg->XferLen = 0u;
                 break;


            case MQTTc_MSG_TYPE_PUBREL:                         /* Finished sending a PUBREL, wait to rx PUBCOMP.       */
                 MQTTc_DBG_TRACE_LOG(("Finished sending Pubrel. Waiting to Rx Pubcomp.\r\n"));
                 p_msg->Type    = MQTTc_MSG_TYPE_PUBCOMP;
                 p_msg->State   = MQTTc_MSG_STATE_WAIT_RX;
                 p_msg->XferLen = 0u;
                 break;


            case MQTTc_MSG_TYPE_PUBCOMP:                        /* Finished sending a PUBCOMP, xfer is cmpl.            */
                 MQTTc_DBG_TRACE_LOG(("Finished sending a Pubcomp. Removing msg from list.\r\n"));
                 p_msg->Type  = MQTTc_MSG_TYPE_PUBLISH;
                 p_msg->State = MQTTc_MSG_STATE_WAIT_RX;
                 p_msg->Err   = MQTTc_ERR_NONE;
                 break;


            case MQTTc_MSG_TYPE_SUBSCRIBE:                      /* Finished sending a SUBSCRIBE, wait to rx SUBACK.     */
                                                                /* Re-obtain nbr of topics in Subscribe msg. See ...    */
                                                                /* Note #1 in MQTTc_SubscribeMult().                    */
                 MQTTc_DBG_TRACE_LOG(("Finished sending Subscribe. Waiting to Rx Suback.\r\n"));


                 p_buf_topic_nbr = ((CPU_INT08U *)p_msg->ArgPtr) - 1u;
                 topic_nbr       =  p_buf_topic_nbr[0u];

                 p_msg->Type    = MQTTc_MSG_TYPE_SUBACK;
                 p_msg->State   = MQTTc_MSG_STATE_WAIT_RX;
                 p_msg->XferLen = topic_nbr;
                 break;


            case MQTTc_MSG_TYPE_UNSUBSCRIBE:                    /* Finished sending a UNSUBSCRIBE, wait to rx UNSUBACK. */
                 MQTTc_DBG_TRACE_LOG(("Finished sending Unsubscribe. Waiting to Rx Unsuback.\r\n"));

                 p_msg->Type    = MQTTc_MSG_TYPE_UNSUBACK;
                 p_msg->State   = MQTTc_MSG_STATE_WAIT_RX;
                 p_msg->XferLen = 0u;
                 break;


            case MQTTc_MSG_TYPE_PINGREQ:                        /* Finished sending a PINGREQ, wait to rx PINGRESP.     */
                 p_msg->Type    = MQTTc_MSG_TYPE_PINGRESP;
                 p_msg->State   = MQTTc_MSG_STATE_WAIT_RX;
                 p_msg->XferLen = 0u;
                 break;


            case MQTTc_MSG_TYPE_DISCONNECT:                     /* Finished sending a DISCONNECT, clear sel descs.      */
                 MQTTc_MsgCallbackExec(p_msg);

                 MQTTc_ConnRemove(p_conn);
                                                                /* Exec callbacks for msgs q'd under this conn.         */
                 MQTTc_MsgListClosedCallbackExec(p_conn->TxMsgHeadPtr);
                 p_conn->TxMsgHeadPtr = DEF_NULL;               /* Mark list as empty.                                  */
                 break;


            default:
                 MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! Wait Tx Cmpl switch, in default case.\n\r"));
                 break;
        }
    }
}


/*
*********************************************************************************************************
*                                         MQTTc_RdSockProcess()
*
* Description : Process read operations required for given MQTTc Connection.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object for which to process read operations.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_Task().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_RdSockProcess (MQTTc_CONN  *p_conn)
{
    MQTTc_MSG   *p_next_msg;
    CPU_INT08U  *p_buf;
    CPU_INT32U   rx_len;
    MQTTc_ERR    err_mqttc;


    if (p_conn->NextMsgPtr == DEF_NULL) {                       /* If next msg is already known, skip this step.        */
        if (p_conn->NextMsgHeader == DEF_BIT_NONE) {

            (void)MQTTc_SockRx(p_conn,                          /* Read header (type, DUP, QoS and retain) of rx'd msg. */
                              &p_conn->NextMsgHeader,
                               1u,
                              &err_mqttc);
            if (err_mqttc == MQTTc_ERR_FATAL) {
                goto err_remove_conn_close_sock;
            } else if (err_mqttc != MQTTc_ERR_NONE) {           /* Wait for more data to be avail to continue.          */
                return;
            }

            MQTTc_DBG_TRACE_DBG(("Rx'd msg type %i.\r\n", ((CPU_INT08U)(p_conn->NextMsgHeader & MQTT_MSG_TYPE_MSK) >> 4u)));
                                                                /* Convert msg type to enum type.                       */
            switch (p_conn->NextMsgHeader & MQTT_MSG_TYPE_MSK) {
                case MQTT_MSG_TYPE_CONNACK:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_CONNACK;
                     MQTTc_DBG_TRACE_LOG(("Connack event rx'd on sock ID %i.\n\r", p_conn->SockId));
                     break;

                case MQTT_MSG_TYPE_PUBLISH:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_PUBLISH;
                     MQTTc_DBG_TRACE_LOG(("Publish event rx'd on sock ID %i.\n\r", p_conn->SockId));
                     break;

                case MQTT_MSG_TYPE_PUBACK:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_PUBACK;
                     MQTTc_DBG_TRACE_LOG(("Puback event rx'd on sock ID %i.\n\r", p_conn->SockId));
                     break;

                case MQTT_MSG_TYPE_PUBREC:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_PUBREC;
                     MQTTc_DBG_TRACE_LOG(("Pubrec event rx'd on sock ID %i.\n\r", p_conn->SockId));
                     break;

                case MQTT_MSG_TYPE_PUBREL:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_PUBREL;
                     MQTTc_DBG_TRACE_LOG(("Pubrel event rx'd on sock ID %i.\n\r", p_conn->SockId));
                     break;

                case MQTT_MSG_TYPE_PUBCOMP:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_PUBCOMP;
                     MQTTc_DBG_TRACE_LOG(("Pubcomp event rx'd on sock ID %i.\n\r", p_conn->SockId));
                     break;

                case MQTT_MSG_TYPE_SUBACK:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_SUBACK;
                     MQTTc_DBG_TRACE_LOG(("Suback event rx'd on sock ID %i.\n\r", p_conn->SockId));
                     break;

                case MQTT_MSG_TYPE_UNSUBACK:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_UNSUBACK;
                     MQTTc_DBG_TRACE_LOG(("Unsuback event rx'd on sock ID %i.\n\r", p_conn->SockId));
                     break;

                case MQTT_MSG_TYPE_PINGRESP:
                     p_conn->NextMsgType = MQTTc_MSG_TYPE_PINGRESP;
                     break;


                case MQTT_MSG_TYPE_CONNECT:
                case MQTT_MSG_TYPE_SUBSCRIBE:
                case MQTT_MSG_TYPE_UNSUBSCRIBE:
                case MQTT_MSG_TYPE_PINGREQ:
                case MQTT_MSG_TYPE_DISCONNECT:
                default:
                     MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! Received wrong message type: %i\r\n", p_conn->NextMsgHeader));
                     p_conn->NextMsgHeader = 0u;
                     goto err_restart;                          /* These msg types cannot be rx'd. Flush rx buf.        */
            }
            p_conn->NextMsgRxLen = 0u;
        }
                                                                /* Make sure msg being rx'd is expected.                */
        if (p_conn->NextMsgType != p_conn->PublishRxMsgPtr->Type) {
            if (p_conn->TxMsgHeadPtr != DEF_NULL) {
                if (p_conn->NextMsgType != p_conn->TxMsgHeadPtr->Type) {
                    goto err_restart;
                }
            } else {
                goto err_restart;
            }
        }

        if (p_conn->NextMsgLenIsCmpl == DEF_NO) {
            CPU_INT08U  rem_len;
            CPU_INT32U  rx_len;
            CPU_INT32U  multiplier;


            do {                                                /* Read rem len of msg. This can be a multi-byte field. */
                rx_len = MQTTc_SockRx(p_conn,
                                     &rem_len,
                                      1u,
                                     &err_mqttc);
                if (err_mqttc == MQTTc_ERR_FATAL) {
                    goto err_remove_conn_close_sock;
                } else if (err_mqttc != MQTTc_ERR_NONE) {       /* Wait for more data to be avail to continue.          */
                    return;
                }
                                                                /* Calculate the multiplier which is a power of 128.    */
                multiplier            = 1 << (7 * p_conn->NextMsgRxLen);
                p_conn->NextMsgRxLen += rx_len;
                                                                /* Use the multiplier to determine msg length           */
                p_conn->NextMsgLen += (rem_len & MQTT_MSG_FIXED_HDR_REM_LEN_MSK) * multiplier;

                                                                /* Read msg as long as continuation bit is set, max 4x. */
            }  while ((DEF_BIT_IS_SET(rem_len, MQTT_MSG_FIXED_HDR_REM_LEN_CONTINUATION_BIT) == DEF_YES) &&
                      (p_conn->NextMsgRxLen                                                 <  MQTT_MSG_FIXED_HDR_REM_LEN_NBR_BYTES_MAX));

            p_conn->NextMsgLenIsCmpl = DEF_YES;
            p_conn->NextMsgRxLen     = 0u;

            MQTTc_DBG_TRACE_DBG(("Finished reading msg len: %i on sock ID %i.\n\r", p_conn->NextMsgLen, p_conn->SockId));
        }

        if ( (p_conn->NextMsgMsgID_IsCmpl == DEF_NO) &&
            ((p_conn->NextMsgType         == MQTTc_MSG_TYPE_PUBACK)  ||
             (p_conn->NextMsgType         == MQTTc_MSG_TYPE_PUBREC)  ||
             (p_conn->NextMsgType         == MQTTc_MSG_TYPE_PUBREL)  ||
             (p_conn->NextMsgType         == MQTTc_MSG_TYPE_PUBCOMP) ||
             (p_conn->NextMsgType         == MQTTc_MSG_TYPE_SUBACK)  ||
             (p_conn->NextMsgType         == MQTTc_MSG_TYPE_UNSUBACK))) {
            CPU_INT08U  msg_id_rx[MQTT_MSG_ID_SIZE];


            if (p_conn->NextMsgRxLen == 1u) {                   /* If already rx'd 1 byte of MsgID, re-put in msg_id_rx.*/
                msg_id_rx[0u] = (p_conn->NextMsgMsgID & 0xFF00u) >> 8u;
            }

            rx_len = MQTTc_SockRx(p_conn,                       /* Rx msg ID if msg has one.                            */
                                 &msg_id_rx[p_conn->NextMsgRxLen],
                                 (MQTT_MSG_ID_SIZE - p_conn->NextMsgRxLen),
                                &err_mqttc);
            if (err_mqttc == MQTTc_ERR_NONE) {
                p_conn->NextMsgRxLen += rx_len;
            } else if (err_mqttc == MQTTc_ERR_FATAL) {
                goto err_remove_conn_close_sock;
            } else {                                            /* Wait to be able to rx data to continue.              */
                if ((err_mqttc == MQTTc_ERR_RX_BUF_EMPTY) &&
                    (rx_len    == 1u)) {                        /* Keep first part of msg ID rx'd.                      */
                    p_conn->NextMsgMsgID = (msg_id_rx[0u] << 8u);
                    p_conn->NextMsgRxLen += 1u;
                }
                return;
            }

            p_conn->NextMsgMsgID         = ((msg_id_rx[0u] << 8u) | msg_id_rx[1u]);
            p_conn->NextMsgLen          -=   MQTT_MSG_ID_SIZE;
            p_conn->NextMsgMsgID_IsCmpl  =   DEF_YES;
            p_conn->NextMsgRxLen         =   0u;

            MQTTc_DBG_TRACE_LOG(("Finished reading next msg msg ID.\n\r"));
        }

        if (p_conn->NextMsgType == p_conn->PublishRxMsgPtr->Type) {
            p_conn->NextMsgPtr   = p_conn->PublishRxMsgPtr;
                                                                /* Account for header that may need to be sent.         */
                                                                /* Start rx'ing useful data at offset, to leave room.   */
            p_conn->NextMsgRxLen = MQTTc_PUBLISH_RX_MSG_BUF_OFFSET;
            if ((p_conn->NextMsgLen + MQTTc_PUBLISH_RX_MSG_BUF_OFFSET) > p_conn->NextMsgPtr->BufLen) {
                MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! Next msg len of Publish Msg (%i (+4 from header)) too big for msg buf (%i).\n\r",
                                       p_conn->NextMsgLen,
                                       p_conn->NextMsgPtr->BufLen));
                p_conn->NextMsgPtr->Err = MQTTc_ERR_BUF_OVERFLOW;
                goto err_callback_restart;
            }
        } else {
            p_conn->NextMsgPtr = p_conn->TxMsgHeadPtr;

            if (p_conn->NextMsgLen != p_conn->NextMsgPtr->XferLen) {
                MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! Next msg len (%i) not equal to expected xfer len (%i).\n\r",
                                       p_conn->NextMsgLen,
                                       p_conn->NextMsgPtr->XferLen));
                p_conn->NextMsgPtr->Err = MQTTc_ERR_BUF_OVERFLOW;
                goto err_callback_restart;
            }
            if (p_conn->NextMsgLen > p_conn->NextMsgPtr->BufLen) {
                MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! Next msg len (%i) too big for msg buf (%i).\n\r",
                                       p_conn->NextMsgLen,
                                       p_conn->NextMsgPtr->BufLen));
                p_conn->NextMsgPtr->Err = MQTTc_ERR_BUF_OVERFLOW;
                goto err_callback_restart;
            }
        }
    }

                                                                /* At this point, p_conn->NextMsgPtr contains the ...   */
                                                                /* next msg to process and all its infos have been ...  */
                                                                /* rx'd. Only the payload still needs to be rx'd.       */
    p_next_msg = p_conn->NextMsgPtr;

    if (p_conn->NextMsgLen != 0u) {                             /* If there is more than the hdr to rx, rx it.          */
        MQTTc_DBG_TRACE_DBG(("Rx'ing payload. Trying to read %i bytes. Already rx'd %i bytes.\n\r", p_conn->NextMsgLen, p_conn->NextMsgRxLen));

        rx_len = MQTTc_SockRx(    p_conn,
                              &(((CPU_INT08U *)p_next_msg->ArgPtr)[p_conn->NextMsgRxLen]),
                                  p_conn->NextMsgLen,
                                 &err_mqttc);
        p_conn->NextMsgLen   -= rx_len;
        p_conn->NextMsgRxLen += rx_len;
        if (err_mqttc == MQTTc_ERR_FATAL) {
            goto err_remove_conn_close_sock;
        } else if (err_mqttc != MQTTc_ERR_NONE) {               /* Wait for more data to be avail to continue.          */
            return;
        }
    }

    MQTTc_DBG_TRACE_DBG(("Finished rx'ing msg payload. Rx'd %i bytes.\n\r", p_conn->NextMsgRxLen));

                                                                /* At this point, the payload has been completely rx'd. */
    MQTTc_DBG_GLOBAL_BUF_COPY((CPU_INT08U *)p_next_msg->ArgPtr, p_conn->NextMsgRxLen);


    if (p_next_msg->Type == MQTTc_MSG_TYPE_PUBLISH) {           /* Rx'd a Publish msg from broker.                      */
                                                                /* 'p_next_msg' points to p_conn->PublishRxMsgPtr.      */
        p_next_msg->QoS = (p_conn->NextMsgHeader & MQTT_MSG_FIXED_HDR_FLAGS_QOS_LVL_MSK) >> MQTT_MSG_FIXED_HDR_FLAGS_QOS_LVL_BIT_SHIFT;


                                                                /* Null-terminate rx'd msg payload.                     */
        ((CPU_INT08U*)(p_next_msg->ArgPtr))[p_conn->NextMsgRxLen] = '\0';

        if (p_next_msg->QoS == 0u) {                            /* If QoS is 0, msg is cmpl'd. Exec callback.           */
            MQTTc_DBG_TRACE_LOG(("MQTTc - Read a Publish (QoS=0) successfully. Executing callback.\n\r"));
            p_next_msg->Err = MQTTc_ERR_NONE;
            MQTTc_MsgCallbackExec(p_next_msg);
        } else {
            MQTTc_MSG_TYPE  type   = MQTTc_MSG_TYPE_PUBREC;
            CPU_INT16U      msg_id;
            CPU_INT16U      len;


            len    = MQTT_MSG_UTF8_LEN_RD(&(((CPU_INT08U *)p_next_msg->ArgPtr)[MQTTc_PUBLISH_RX_MSG_BUF_OFFSET])) + MQTT_MSG_UTF8_LEN_SIZE;
            msg_id = MQTT_MSG_UTF8_LEN_RD(&(((CPU_INT08U *)p_next_msg->ArgPtr)[len + MQTTc_PUBLISH_RX_MSG_BUF_OFFSET]));

            if (p_next_msg->QoS == 1u) {                        /* Callback must be called now only for QoS 1.          */
                p_next_msg->Err = MQTTc_ERR_NONE;
                MQTTc_MsgCallbackExec(p_next_msg);

                type = MQTTc_MSG_TYPE_PUBACK;
            }

                                                                /* Cfg fixed hdr section of msg for reply.              */
            p_buf = MQTTc_FixedHdrBufCfg((CPU_INT08U *)p_next_msg->ArgPtr,
                                                       type,
                                                       DEF_NO,
                                                       0u,
                                                       DEF_NO,
                                                       MQTT_MSG_ID_SIZE,
                                                      &p_next_msg->Err);

            p_buf[0u] = (msg_id >>    8u);
            p_buf[1u] = (msg_id &  0xFFu);

            MQTTc_DBG_TRACE_LOG(("MQTTc - Read a Publish (QoS=%i) successfully. Sending a Puback/Pubrec with Msg ID: %i.\n\r", p_next_msg->QoS, msg_id));

            MQTTc_SockSelDescSet(p_conn, MQTTc_SEL_DESC_TYPE_WR);

            p_next_msg->Type    = type;
            p_next_msg->State   = MQTTc_MSG_STATE_MUST_TX;
            p_next_msg->XferLen = MQTT_MSG_BASE_LEN;
            p_next_msg->MsgID   = msg_id;
            p_next_msg->Err     = MQTTc_ERR_NONE;
        }

        MQTTc_ConnNextMsgClr(p_conn);                           /* Clr NextMsg fields.                                  */

        return;
    } else {
        CPU_INT08U  *p_buf_topic_nbr;
        CPU_INT08U   topic_nbr;
        CPU_INT08U   topic_ix;


        switch (p_next_msg->Type) {
            case MQTTc_MSG_TYPE_CONNACK:
                 if ((p_conn->NextMsgRxLen                   != 2u) ||
                     (((CPU_INT08U *)p_next_msg->ArgPtr)[1u] != MQTT_MSG_VAR_HDR_CONNACK_RET_CODE_ACCEPTED)) {
                     MQTTc_DBG_TRACE_DBG(("MQTTc - Connack code not OK.\n\r"));
                     p_next_msg->Err = MQTTc_ERR_CONNACK_FAIL;
                 } else {
                     MQTTc_DBG_TRACE_DBG(("MQTTc - Connack code OK.\n\r"));
                     p_next_msg->Err = MQTTc_ERR_NONE;
                 }
                 break;


            case MQTTc_MSG_TYPE_PUBACK:
                 if (p_conn->NextMsgRxLen != 0u) {
                     MQTTc_DBG_TRACE_DBG(("MQTTc - Puback rx'd code not OK.\n\r"));
                     p_next_msg->Err = MQTTc_ERR_FAIL;
                 } else {
                     p_next_msg->Err = MQTTc_ERR_NONE;
                 }
                 break;


            case MQTTc_MSG_TYPE_SUBACK:
                 p_next_msg->Err = MQTTc_ERR_NONE;

                 p_buf_topic_nbr = ((CPU_INT08U *)p_next_msg->ArgPtr) - 1u;
                 topic_nbr       =   p_buf_topic_nbr[0u];

                 for (topic_ix = 0u; topic_ix < topic_nbr; topic_ix++) {
                     p_buf_topic_nbr--;
                     if ((*p_buf_topic_nbr) != ((CPU_INT08U *)p_next_msg->ArgPtr)[topic_ix]) {
                         p_next_msg->Err = MQTTc_ERR_QoS_LEVEL_NOT_GRANTED;
                         MQTTc_DBG_TRACE_DBG(("!!! ERROR !!! QoS Level not granted by server. Asked %i, got %i\n\r",
                                               *p_buf_topic_nbr,
                                                p_next_msg->BufPtr[topic_ix]));
                         break;
                     }
                 }

                 if (p_conn->NextMsgRxLen == 0u) {
                     MQTTc_DBG_TRACE_DBG(("MQTTc - Suback rx'd len not OK.\n\r"));
                     p_next_msg->Err = MQTTc_ERR_FAIL;
                 }
                 p_next_msg->ArgPtr = (void *)p_buf_topic_nbr;
                 break;


            case MQTTc_MSG_TYPE_UNSUBACK:
                 if (p_conn->NextMsgRxLen != 0u) {
                     MQTTc_DBG_TRACE_DBG(("MQTTc - Suback rx'd len not OK.\n\r"));
                     p_next_msg->Err = MQTTc_ERR_FAIL;
                 } else {
                     p_next_msg->Err = MQTTc_ERR_NONE;
                 }
                 break;


            case MQTTc_MSG_TYPE_PINGRESP:
                 break;


            case MQTTc_MSG_TYPE_PUBREC:
                 MQTTc_DBG_TRACE_LOG(("Pubrec event rx'd, removing msg from list."));

                 p_buf = MQTTc_FixedHdrBufCfg((CPU_INT08U *)p_next_msg->ArgPtr,
                                                            MQTTc_MSG_TYPE_PUBREL,
                                                            DEF_NO,
                                                            1u,
                                                            DEF_NO,
                                                            MQTT_MSG_ID_SIZE,
                                                           &p_next_msg->Err);

                 p_buf[0u] = (p_next_msg->MsgID >>    8u);
                 p_buf[1u] = (p_next_msg->MsgID &  0xFFu);

                 MQTTc_SockSelDescSet(p_conn, MQTTc_SEL_DESC_TYPE_WR);

                 p_next_msg->Type    = MQTTc_MSG_TYPE_PUBREL;
                 p_next_msg->State   = MQTTc_MSG_STATE_MUST_TX;
                 p_next_msg->XferLen = MQTT_MSG_BASE_LEN;
                 p_next_msg->Err     = MQTTc_ERR_NONE;

                 MQTTc_ConnNextMsgClr(p_conn);                  /* Clr NextMsg fields.                                  */
                 return;


            case MQTTc_MSG_TYPE_PUBREL:
                 MQTTc_DBG_TRACE_LOG(("Pubrel event rx'd, removing msg from list."));

                 MQTTc_MsgCallbackExec(p_next_msg);

                 p_buf = MQTTc_FixedHdrBufCfg((CPU_INT08U *)p_next_msg->ArgPtr,
                                                            MQTTc_MSG_TYPE_PUBCOMP,
                                                            DEF_NO,
                                                            1u,
                                                            DEF_NO,
                                                            MQTT_MSG_ID_SIZE,
                                                           &p_next_msg->Err);

                 p_buf[0u] = (p_next_msg->MsgID >>    8u);
                 p_buf[1u] = (p_next_msg->MsgID &  0xFFu);

                 MQTTc_SockSelDescSet(p_conn, MQTTc_SEL_DESC_TYPE_WR);

                 p_next_msg->Type    = MQTTc_MSG_TYPE_PUBCOMP;
                 p_next_msg->State   = MQTTc_MSG_STATE_MUST_TX;
                 p_next_msg->XferLen = MQTT_MSG_BASE_LEN;
                 p_next_msg->Err     = MQTTc_ERR_NONE;

                 MQTTc_ConnNextMsgClr(p_conn);                  /* Clr NextMsg fields.                                  */
                 return;


            case MQTTc_MSG_TYPE_PUBCOMP:
                 if (p_conn->NextMsgRxLen != 0u) {
                     MQTTc_DBG_TRACE_DBG(("MQTTc - Pubcomp rx'd len not OK.\n\r"));
                     p_next_msg->Err = MQTTc_ERR_FAIL;
                 } else {
                     p_next_msg->Err = MQTTc_ERR_NONE;
                 }
                 break;


            default:
                 MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! In Rx switch default case.\n\r"));
                 break;
        }

        MQTTc_ConnNextMsgClr(p_conn);                           /* Clr NextMsg fields.                                  */

        MQTTc_MsgCallbackExec(p_next_msg);
    }

    return;

err_callback_restart:
    MQTTc_MsgCallbackExec(p_conn->NextMsgPtr);
    MQTTc_ConnNextMsgClr(p_conn);                               /* Clr NextMsg fields.                                  */

    return;

err_restart:
    MQTTc_ConnNextMsgClr(p_conn);                               /* Clr NextMsg fields.                                  */
    err_mqttc = MQTTc_ERR_UNEXPECTED_MSG;

err_remove_conn_close_sock:
    {
        MQTTc_ERR_CALLBACK   on_err_callback;
        void                *p_arg;
        MQTTc_ERR            close_err;


        on_err_callback = p_conn->OnErrCallback;
        p_arg           = p_conn->ArgPtr;

        MQTTc_ConnCloseProc(p_conn,
                           &close_err);
        (void)close_err;

        if (on_err_callback != DEF_NULL) {
            on_err_callback(p_conn,
                            p_arg,
                            err_mqttc);
        }
    }

    return;
}


/*
*********************************************************************************************************
*                                          MQTTc_MsgProcess()
*
* Description : Process message pending and enqueuing for MQTTc task.
*
* Argument(s) : none.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_Task().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_MsgProcess (void)
{
    MQTTc_MSG  *p_msg;


    p_msg = MQTTc_MsgCheck();
    if (p_msg != DEF_NULL) {
        if (p_msg->Type != MQTTc_MSG_TYPE_REQ_CLOSE) {
            MQTTc_CONN      *p_conn = p_msg->ConnPtr;
            MQTTc_MSG_TYPE   type   = p_msg->Type;


            if (p_conn->TxMsgHeadPtr == DEF_NULL) {                 /* Enqueue msg to appropriate MQTTc conn.               */
                p_conn->TxMsgHeadPtr = p_msg;
            } else {
                MQTTc_MSG  *p_iter_msg = p_conn->TxMsgHeadPtr;


                while (p_iter_msg->NextPtr != DEF_NULL) {
                    p_iter_msg = p_iter_msg->NextPtr;
                }
                p_iter_msg->NextPtr = p_msg;
            }

            switch (type) {
                case MQTTc_MSG_TYPE_CONNECT:
                     if (MQTTc_Ptr->ConnHeadPtr == DEF_NULL) {       /* Enqueue conn in MQTTc conn list.                     */
                         MQTTc_Ptr->ConnHeadPtr = p_conn;
                     } else {
                         MQTTc_CONN  *p_iter_conn = MQTTc_Ptr->ConnHeadPtr;


                         while (p_iter_conn->NextPtr != DEF_NULL) {
                             p_iter_conn = p_iter_conn->NextPtr;
                         }
                         p_iter_conn->NextPtr = p_conn;
                     }
                                                                    /* break intentionally omitted.                         */
                case MQTTc_MSG_TYPE_PUBLISH:
                case MQTTc_MSG_TYPE_PUBREL:
                case MQTTc_MSG_TYPE_SUBSCRIBE:
                case MQTTc_MSG_TYPE_UNSUBSCRIBE:
                case MQTTc_MSG_TYPE_PINGREQ:
                case MQTTc_MSG_TYPE_DISCONNECT:
                     MQTTc_SockSelDescSet(p_conn, MQTTc_SEL_DESC_TYPE_WR);
                     break;


                case MQTTc_MSG_TYPE_PUBACK:
                case MQTTc_MSG_TYPE_PUBREC:
                case MQTTc_MSG_TYPE_PUBCOMP:
                case MQTTc_MSG_TYPE_CONNACK:
                case MQTTc_MSG_TYPE_SUBACK:
                case MQTTc_MSG_TYPE_UNSUBACK:
                case MQTTc_MSG_TYPE_PINGRESP:
                default:
                     MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! In default case for event type:%i\n\r", type));
                     break;
            }
        } else {                                                /* Handle special close req msg.                        */
            KAL_ERR  err_kal;


            MQTTc_ConnCloseProc(p_msg->ConnPtr,
                               &p_msg->Err);

            KAL_SemPost((*(KAL_SEM_HANDLE *)p_msg->ArgPtr),
                        KAL_OPT_POST_NONE,
                       &err_kal);
            (void)&err_kal;
        }
    }
}


/*
*********************************************************************************************************
*                                        MQTTc_MsgCallbackExec()
*
* Description : Frees msg if provided by user and execute application callback(s).
*
* Argument(s) : p_msg           Pointer to message for which the callback must be called.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_RdSockProcess(),
*               MQTTc_WrSockProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_MsgCallbackExec (MQTTc_MSG  *p_msg)
{
    MQTTc_CMPL_CALLBACK   callback_fnct = DEF_NULL;
    MQTTc_CONN           *p_conn        = p_msg->ConnPtr;
    MQTTc_ERR             err           = MQTTc_ERR_NONE;


    if (p_msg != p_conn->PublishRxMsgPtr) {
        switch (p_msg->Type) {                                  /* Find type of msg and if ok to call callback for it.  */
            case MQTTc_MSG_TYPE_CONNECT:
                 err = MQTTc_ERR_FAIL;
                                                                /* break intentionally omitted.                         */
            case MQTTc_MSG_TYPE_CONNACK:
                 p_msg->Type   = MQTTc_MSG_TYPE_CONNECT;
                 callback_fnct = p_conn->OnConnectCmpl;
                 if (p_msg->State != MQTTc_MSG_STATE_WAIT_RX) {
                     err = MQTTc_ERR_FAIL;
                 }
                 break;


            case MQTTc_MSG_TYPE_PUBLISH:
                 callback_fnct = p_conn->OnPublishCmpl;
                 if ((p_msg->QoS   != 0u) ||
                     (p_msg->State != MQTTc_MSG_STATE_WAIT_TX_CMPL)) {
                     err = MQTTc_ERR_FAIL;
                 }
                 break;


            case MQTTc_MSG_TYPE_PUBACK:
            case MQTTc_MSG_TYPE_PUBCOMP:
                 p_msg->Type   = MQTTc_MSG_TYPE_PUBLISH;
                 callback_fnct = p_conn->OnPublishCmpl;
                 if (p_msg->State != MQTTc_MSG_STATE_WAIT_RX) {
                     err = MQTTc_ERR_FAIL;
                 }
                 break;


            case MQTTc_MSG_TYPE_PUBREC:
            case MQTTc_MSG_TYPE_PUBREL:
                 p_msg->Type   = MQTTc_MSG_TYPE_PUBLISH;
                 callback_fnct = p_conn->OnPublishCmpl;
                 err = MQTTc_ERR_FAIL;
                 break;


            case MQTTc_MSG_TYPE_SUBSCRIBE:
                 err = MQTTc_ERR_FAIL;
                                                                /* break intentionally omitted.                         */
            case MQTTc_MSG_TYPE_SUBACK:
                 p_msg->Type   = MQTTc_MSG_TYPE_SUBSCRIBE;
                 callback_fnct = p_conn->OnSubscribeCmpl;
                 if (p_msg->State != MQTTc_MSG_STATE_WAIT_RX) {
                     err = MQTTc_ERR_FAIL;
                 }
                 break;


            case MQTTc_MSG_TYPE_UNSUBSCRIBE:
                 err = MQTTc_ERR_FAIL;
                                                                /* break intentionally omitted.                         */
            case MQTTc_MSG_TYPE_UNSUBACK:
                 p_msg->Type   = MQTTc_MSG_TYPE_UNSUBSCRIBE;
                 callback_fnct = p_conn->OnUnsubscribeCmpl;
                 if (p_msg->State != MQTTc_MSG_STATE_WAIT_RX) {
                     err = MQTTc_ERR_FAIL;
                 }
                 break;


            case MQTTc_MSG_TYPE_PINGREQ:
                 err = MQTTc_ERR_FAIL;
            case MQTTc_MSG_TYPE_PINGRESP:
                 p_msg->Type   = MQTTc_MSG_TYPE_PINGREQ;
                 callback_fnct = p_conn->OnPingReqCmpl;
                 if (p_msg->State != MQTTc_MSG_STATE_WAIT_RX) {
                     err = MQTTc_ERR_FAIL;
                 }
                 break;


            case MQTTc_MSG_TYPE_DISCONNECT:
                 p_msg->Type   = MQTTc_MSG_TYPE_DISCONNECT;
                 callback_fnct = p_conn->OnDisconnectCmpl;
                 if (p_msg->State != MQTTc_MSG_STATE_WAIT_TX_CMPL) {
                     err = MQTTc_ERR_FAIL;
                 }
                 break;


            default:
                 MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! In default case for p_event->Type when executing callback:%i\n\r", p_msg->Type));
                 return;
        }

        if (err != MQTTc_ERR_NONE) {
            MQTTc_DBG_TRACE_DBG(("!!! ERROR !!! Detected error when executing callback: %i\n\r", err));
        }

        if (p_msg->Err == MQTTc_ERR_NONE) {
            p_msg->Err = err;
        } else  {
            MQTTc_DBG_TRACE_DBG(("MQTTc - Callback executed for Msg with error: %i\n\r", p_msg->Err));
        }

        p_msg->State = MQTTc_MSG_STATE_CMPL;

        MQTTc_MsgID_Free(p_msg->MsgID);                         /* Free msg ID, if any.                                 */

                                                                /* Remove msg from conn's msg list.                     */
                                                                /* Msg is necessarily located at head of list.          */
        p_msg->ConnPtr->TxMsgHeadPtr = p_msg->NextPtr;
        p_msg->NextPtr               = DEF_NULL;

        if (p_conn->OnCmpl != DEF_NULL) {                       /* Call generic callback, if not NULL.                  */
            p_conn->OnCmpl(p_conn,
                           p_msg,
                           p_conn->ArgPtr,
                           p_msg->Err);
        }

        if (callback_fnct != DEF_NULL) {                        /* Call action-specific callback, if not NULL.          */
            callback_fnct(p_conn,
                          p_msg,
                          p_conn->ArgPtr,
                          p_msg->Err);
        }
    } else if (p_conn->OnPublishRx != DEF_NULL) {               /* Call OnPublishRx callback, if not NULL.              */
        CPU_INT08U  *p_buf_start   = &(((CPU_INT08U *)p_msg->ArgPtr)[MQTTc_PUBLISH_RX_MSG_BUF_OFFSET]);
        CPU_INT08U  *p_buf_topic   = &p_buf_start[MQTT_MSG_UTF8_LEN_SIZE];
        CPU_INT08U  *p_buf_payload;
        CPU_INT32U   topic_len;
        CPU_INT32U   payload_len;
        CPU_INT32U   len;


        MQTTc_DBG_GLOBAL_BUF_COPY(p_buf_start, 512u);

        topic_len = MQTT_MSG_UTF8_LEN_RD(p_buf_start);
        len       = topic_len + MQTT_MSG_UTF8_LEN_SIZE;         /* Account for length.                                  */

        if (p_msg->QoS != 0u) {                                 /* Account for msg ID size if needed.                   */
            len += MQTT_MSG_ID_SIZE;
        }

        payload_len   =  p_msg->ConnPtr->NextMsgRxLen - len;
        p_buf_payload = &p_buf_start[len];

        p_conn->OnPublishRx(                  p_conn,
                            (const CPU_CHAR *)p_buf_topic,
                                              topic_len,
                            (const CPU_CHAR *)p_buf_payload,
                                              payload_len,
                                              p_conn->ArgPtr,
                                              p_msg->Err);
    }

    return;
}


/*
*********************************************************************************************************
*                                           MQTTc_MsgCheck()
*
* Description : See if a message is available in the message queue.
*
* Argument(s) : none.
*
* Return(s)   : Pointer to obtained message, if any,
*               DEF_NULL,                    otherwise.
*
* Caller(s)   : MQTTc_MsgProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  MQTTc_MSG  *MQTTc_MsgCheck (void)
{
    MQTTc_MSG  *p_msg = DEF_NULL;
    CPU_SR_ALLOC();


    CPU_CRITICAL_ENTER();
    if (MQTTc_Ptr->MsgListHeadPtr != DEF_NULL) {
        p_msg                     = MQTTc_Ptr->MsgListHeadPtr;
        MQTTc_Ptr->MsgListHeadPtr = p_msg->NextPtr;
        p_msg->NextPtr            = DEF_NULL;
    }
    CPU_CRITICAL_EXIT();

    return (p_msg);
}


/*
*********************************************************************************************************
*                                            MQTTc_MsgPost()
*
* Description : Add a message in the MQTT message queue for the task to process.
*
* Argument(s) : p_conn          Pointer to MQTT Connection object associated with message.
*
*               p_msg           Pointer to MQTT Message object to add to queue.
*
*               type            Type of MQTT message.
*
*               xfer_len        Required len to xfer.
*
*               qos_lvl         QoS level of the message, if any.
*
*               msg_id          Message ID of the message, if any.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_OS_FAIL           OS operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : Various MQTTc functions.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_MsgPost (MQTTc_CONN      *p_conn,
                             MQTTc_MSG       *p_msg,
                             MQTTc_MSG_TYPE   type,
                             CPU_INT32U       xfer_len,
                             CPU_INT08U       qos_lvl,
                             CPU_INT16U       msg_id,
                             MQTTc_ERR       *p_err)
{
    CPU_SR_ALLOC();


    p_msg->ConnPtr = p_conn;                                    /* Set values in msg fields.                            */
    p_msg->Type    = type;
    p_msg->State   = MQTTc_MSG_STATE_MUST_TX;
    p_msg->MsgID   = msg_id;
    p_msg->XferLen = xfer_len;
    p_msg->QoS     = qos_lvl;
    p_msg->Err     = MQTTc_ERR_NONE;
    p_msg->NextPtr = DEF_NULL;

    CPU_CRITICAL_ENTER();
    if (p_conn->SockId != NET_SOCK_ID_NONE) {

        if (MQTTc_Ptr->MsgListHeadPtr != DEF_NULL) {
            MQTTc_Ptr->MsgListTailPtr->NextPtr = p_msg;
        } else {
            MQTTc_Ptr->MsgListHeadPtr = p_msg;
        }
        MQTTc_Ptr->MsgListTailPtr = p_msg;

        CPU_CRITICAL_EXIT();

        MQTTc_SockSelDescSet(p_conn, MQTTc_SEL_DESC_TYPE_WR);

       *p_err = MQTTc_ERR_NONE;
    } else {
        CPU_CRITICAL_EXIT();

       *p_err = MQTTc_ERR_CONN_IS_CLOSED;
    }
    return;
}


/*
*********************************************************************************************************
*                                   MQTTc_MsgListClosedCallbackExec()
*
* Description : Execute callback with error MQTTc_ERR_CONN_IS_CLOSED on every message in linked list.
*
* Argument(s) : p_head_msg      Pointer to head of list of messages to execute callback on.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_ConnCloseProc(),
*               MQTTc_WrSockProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_MsgListClosedCallbackExec (MQTTc_MSG  *p_head_msg)
{
    MQTTc_MSG  *p_iter_msg;
    MQTTc_MSG  *p_next_iter_msg;


    p_iter_msg = p_head_msg;                                    /* Exec callback for every msg in list passed.          */
    while (p_iter_msg != DEF_NULL) {

        p_next_iter_msg = p_iter_msg->NextPtr;
        p_iter_msg->Err = MQTTc_ERR_CONN_IS_CLOSED;             /* Indicate conn is closed.                             */
        MQTTc_MsgCallbackExec(p_iter_msg);

        p_iter_msg = p_next_iter_msg;
    }
}


/*
*********************************************************************************************************
*                                        MQTTc_FixedHdrBufCfg()
*
* Description : Fill buffer with correct fields of fixed header.
*
* Argument(s) : p_buf           Pointer to beginning of the buffer to fill.
*
*               msg_type        Type of message.
*
*               dup_flag        DUP flag.
*
*               qos_lvl         QoS level of the message.
*
*               retain_flag     Retain flag.
*
*               rem_len         Remaining length of the message.
*
*               p_err           Pointer to variable that will receive the return error code.
*               -----           Argument validated by caller.
*                                   MQTTc_ERR_NONE              Operation successful.
*                                   MQTTc_ERR_NULL_PTR          Null ptr was passed as argument.
*                                   MQTTc_ERR_INVALID_ARG       Invalid arg passed to function.
*
* Return(s)   : Pointer to next location in buffer, if NO error(s),
*               DEF_NULL,                           otherwise.
*
* Caller(s)   : Various MQTTc functions.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  CPU_INT08U  *MQTTc_FixedHdrBufCfg (CPU_INT08U      *p_buf,
                                           MQTTc_MSG_TYPE   msg_type,
                                           CPU_BOOLEAN      dup_flag,
                                           CPU_INT08U       qos_lvl,
                                           CPU_BOOLEAN      retain_flag,
                                           CPU_INT32U       rem_len,
                                           MQTTc_ERR       *p_err)
{
    CPU_INT08U  *p_cur_buf;
    CPU_INT08U   encoded_byte;
    CPU_INT32U   len;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_buf == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return (0u);
        }

        if (rem_len > MQTT_MSG_FIXED_HDR_REM_LEN_MAX) {         /* Make sure len can be encoded.                        */
           *p_err = MQTTc_ERR_INVALID_ARG;
            return (0u);
        }
    #endif

    p_cur_buf = p_buf;
    len       = rem_len;

    switch (msg_type) {                                         /* Convert msg type.                                    */
        case MQTTc_MSG_TYPE_CONNECT:
            *p_cur_buf = MQTT_MSG_TYPE_CONNECT;
             break;

        case MQTTc_MSG_TYPE_CONNACK:
            *p_cur_buf = MQTT_MSG_TYPE_CONNACK;
             break;

        case MQTTc_MSG_TYPE_PUBLISH:
            *p_cur_buf = MQTT_MSG_TYPE_PUBLISH;
             break;

        case MQTTc_MSG_TYPE_PUBACK:
            *p_cur_buf = MQTT_MSG_TYPE_PUBACK;
             break;

        case MQTTc_MSG_TYPE_PUBREC:
            *p_cur_buf = MQTT_MSG_TYPE_PUBREC;
             break;

        case MQTTc_MSG_TYPE_PUBREL:
            *p_cur_buf = MQTT_MSG_TYPE_PUBREL;
             break;

        case MQTTc_MSG_TYPE_PUBCOMP:
            *p_cur_buf = MQTT_MSG_TYPE_PUBCOMP;
             break;

        case MQTTc_MSG_TYPE_SUBSCRIBE:
            *p_cur_buf = MQTT_MSG_TYPE_SUBSCRIBE;
             break;

        case MQTTc_MSG_TYPE_SUBACK:
            *p_cur_buf = MQTT_MSG_TYPE_SUBACK;
             break;

        case MQTTc_MSG_TYPE_UNSUBSCRIBE:
            *p_cur_buf = MQTT_MSG_TYPE_UNSUBSCRIBE;
             break;

        case MQTTc_MSG_TYPE_UNSUBACK:
            *p_cur_buf = MQTT_MSG_TYPE_UNSUBACK;
             break;

        case MQTTc_MSG_TYPE_PINGREQ:
            *p_cur_buf = MQTT_MSG_TYPE_PINGREQ;
             break;

        case MQTTc_MSG_TYPE_PINGRESP:
            *p_cur_buf = MQTT_MSG_TYPE_PINGRESP;
             break;

        case MQTTc_MSG_TYPE_DISCONNECT:
            *p_cur_buf = MQTT_MSG_TYPE_DISCONNECT;
             break;

        default:
             break;
    }


    switch (msg_type) {
        case MQTTc_MSG_TYPE_PUBLISH:                            /* Set every val.                                       */
             if (retain_flag == DEF_YES) {
                 DEF_BIT_SET(*p_cur_buf, MQTT_MSG_FIXED_HDR_FLAGS_RETAIN_MSK);
             } else {
                 DEF_BIT_CLR(*p_cur_buf, MQTT_MSG_FIXED_HDR_FLAGS_RETAIN_MSK);
             }
                                                                /* break intentionnally omitted.                        */
        case MQTTc_MSG_TYPE_PUBREL:                             /* Set everything except 'retain'.                      */
        case MQTTc_MSG_TYPE_SUBSCRIBE:
        case MQTTc_MSG_TYPE_UNSUBSCRIBE:
             if (dup_flag == DEF_YES) {
                 DEF_BIT_SET(*p_cur_buf, MQTT_MSG_FIXED_HDR_FLAGS_DUP_MSK);
             } else {
                 DEF_BIT_CLR(*p_cur_buf, MQTT_MSG_FIXED_HDR_FLAGS_DUP_MSK);
             }
             DEF_BIT_SET(*p_cur_buf, (qos_lvl << MQTT_MSG_FIXED_HDR_FLAGS_QOS_LVL_BIT_SHIFT));
                                                                /* break intentionnally omitted.                        */
        case MQTTc_MSG_TYPE_CONNECT:                            /* Set only the msg_type.                               */
        case MQTTc_MSG_TYPE_PUBACK:
        case MQTTc_MSG_TYPE_PUBREC:
        case MQTTc_MSG_TYPE_PUBCOMP:
        case MQTTc_MSG_TYPE_PINGREQ:
        case MQTTc_MSG_TYPE_DISCONNECT:
             break;


        case MQTTc_MSG_TYPE_CONNACK:                            /* Err cases.                                           */
        case MQTTc_MSG_TYPE_SUBACK:
        case MQTTc_MSG_TYPE_UNSUBACK:
        case MQTTc_MSG_TYPE_PINGRESP:
        default:
             MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! In default case for msg_type:%i\n\r", msg_type));
             break;
    }

    do {
        encoded_byte = (len) % (MQTT_MSG_FIXED_HDR_REM_LEN_MAX_LEN);

        len = len / MQTT_MSG_FIXED_HDR_REM_LEN_MAX_LEN;

        if (len > 0) {
            encoded_byte = encoded_byte | MQTT_MSG_FIXED_HDR_REM_LEN_MAX_LEN;
        }
        p_cur_buf++;
       *p_cur_buf = encoded_byte;
    } while (len > 0);

    p_cur_buf++;

   *p_err = MQTTc_ERR_NONE;

    return (p_cur_buf);
}


/*
*********************************************************************************************************
*                                           MQTTc_MsgID_Get()
*
* Description : Obtain a msg ID to use for a message requiring one.
*
* Argument(s) : none.
*
* Return(s)   : Message ID,          if NO error(s),
*               MQTT_MSG_ID_INVALID, otherwise.
*
* Caller(s)   : MQTTc_Publish(),
*               MQTTc_SubscribeMult(),
*               MQTTc_UnsubscribeMult().
*
* Note(s)     : (1) Once the message has been completed, MQTTc_MsgID_Free() must be called to release the
*                   msg ID so that other messages can use it.
*********************************************************************************************************
*/

static  CPU_INT16U   MQTTc_MsgID_Get (void)
{
    CPU_INT16U  msg_id    = MQTT_MSG_ID_INVALID;
    CPU_INT08U  bitmap_ix;
    CPU_SR_ALLOC();


    CPU_CRITICAL_ENTER();
    for (bitmap_ix = 0u; bitmap_ix < MQTTc_Ptr->MsgID_BitmapTblMax; bitmap_ix++) {
        if (DEF_BIT_IS_CLR_ANY(MQTTc_Ptr->MsgID_BitmapTbl[bitmap_ix], DEF_INT_32_MASK) == DEF_YES) {

            msg_id = DEF_INT_32_NBR_BITS - 1u - CPU_CntLeadZeros(~MQTTc_Ptr->MsgID_BitmapTbl[bitmap_ix]);

            DEF_BIT_SET(MQTTc_Ptr->MsgID_BitmapTbl[bitmap_ix], DEF_BIT(msg_id));
            msg_id += (DEF_INT_32_NBR_BITS * bitmap_ix) + 1u;
            break;
        }
    }
    CPU_CRITICAL_EXIT();

    return (msg_id);
}


/*
*********************************************************************************************************
*                                          MQTTc_MsgID_Free()
*
* Description : Free message ID, allowing other messages to use it.
*
* Argument(s) : msg_id          Message ID to release.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_MsgCallbackExec().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_MsgID_Free (CPU_INT16U  msg_id)
{
    CPU_SR_ALLOC();


    if (msg_id != MQTT_MSG_ID_NONE) {
        CPU_CRITICAL_ENTER();
        DEF_BIT_CLR(MQTTc_Ptr->MsgID_BitmapTbl[(msg_id - 1u) / 32u], DEF_BIT((msg_id - 1u) % DEF_INT_32_NBR_BITS));
        CPU_CRITICAL_EXIT();
    }

    return;
}


/*
*********************************************************************************************************
*                                        MQTTc_ConnNextMsgClr()
*
* Description : Clear next message fields in MQTTc Connection object.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object for which to clear the NextMsg fields.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_ConnClr(),
*               MQTTc_RdSockProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_ConnNextMsgClr (MQTTc_CONN  *p_conn)
{
    p_conn->NextMsgHeader       = DEF_BIT_NONE;                 /* Clr NextMsg fields.                                  */
    p_conn->NextMsgRxLen        = 0u;
    p_conn->NextMsgType         = MQTTc_MSG_TYPE_NONE;
    p_conn->NextMsgLen          = 0u;
    p_conn->NextMsgLenIsCmpl    = DEF_NO;
    p_conn->NextMsgMsgID        = MQTT_MSG_ID_NONE;
    p_conn->NextMsgMsgID_IsCmpl = DEF_NO;
    p_conn->NextMsgPtr          = DEF_NULL;

    return;
}


/*
*********************************************************************************************************
*                                         MQTTc_ConnCloseProc()
*
* Description : Process the close operation for a given MQTTc Connection object.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to close.
*
*               p_err           Pointer to variable that will receive the return error code from this function :
*                                   MQTTc_ERR_NONE               Operation successful.
*
*                               --------------- See NetSock_Close() for more error codes. ---------------
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_MsgProcess(),
*               MQTTc_RdSockProcess(),
*               MQTTc_Task().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_ConnCloseProc (MQTTc_CONN  *p_conn,
                                   MQTTc_ERR   *p_err)
{
    MQTTc_MSG  *p_head_callback_msg = DEF_NULL;
    MQTTc_MSG  *p_tail_callback_msg = DEF_NULL;
    MQTTc_MSG  *p_iter_msg;
    MQTTc_MSG  *p_next_iter_msg;
    MQTTc_MSG  *p_prev_iter_msg     = DEF_NULL;
    CPU_SR_ALLOC();


    MQTTc_SockConnClose(p_conn,
                        p_err);

    MQTTc_ConnRemove(p_conn);

    CPU_CRITICAL_ENTER();
    p_conn->SockId = NET_SOCK_ID_NONE;                          /* Mark the conn as unusable.                           */

    p_iter_msg = MQTTc_Ptr->MsgListHeadPtr;
    while (p_iter_msg != DEF_NULL) {                            /* Iterate in list of posted msg.                       */
        p_next_iter_msg = p_iter_msg->NextPtr;

        if (p_iter_msg->ConnPtr == p_conn) {                    /* See if msg was posted on same conn that is closing.  */
            if (p_head_callback_msg == DEF_NULL) {              /* Append msg at list of msg to free.                   */
                p_head_callback_msg = p_iter_msg;
                p_tail_callback_msg = p_iter_msg;
            } else {
                p_tail_callback_msg->NextPtr = p_iter_msg;
                p_tail_callback_msg          = p_iter_msg;
            }

            if (p_prev_iter_msg != DEF_NULL) {                  /* Make sure prev msg is point at correct next msg.     */
                p_prev_iter_msg->NextPtr = p_next_iter_msg;
            } else {
                MQTTc_Ptr->MsgListHeadPtr = p_next_iter_msg;
            }
        } else {
            p_prev_iter_msg = p_iter_msg;
        }

        p_iter_msg = p_next_iter_msg;
    }
    CPU_CRITICAL_EXIT();

    MQTTc_MsgListClosedCallbackExec(p_conn->TxMsgHeadPtr);      /* Exec callbacks for msgs q'd under this conn.         */
    p_conn->TxMsgHeadPtr = DEF_NULL;                            /* Mark list as empty.                                  */

                                                                /* Exec callback, in order, for each msg that had ...   */
    MQTTc_MsgListClosedCallbackExec(p_head_callback_msg);       /* been posted but not processed, for that conn.        */
}


/*
*********************************************************************************************************
*                                          MQTTc_ConnRemove()
*
* Description : Remove MQTTc Connection object from global connection list.
*
* Argument(s) : p_conn          Pointer to MQTTc Connection object to remove from list.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_ConnCloseProc(),
*               MQTTc_WrSockProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  MQTTc_ConnRemove (MQTTc_CONN  *p_conn)
{
    MQTTc_SockSelDescClr(p_conn, MQTTc_SEL_DESC_TYPE_RD);
    MQTTc_SockSelDescClr(p_conn, MQTTc_SEL_DESC_TYPE_WR);
    MQTTc_SockSelDescClr(p_conn, MQTTc_SEL_DESC_TYPE_ERR);

    if (MQTTc_Ptr->ConnHeadPtr == p_conn) {                     /* If conn is located at head of list.                  */
        MQTTc_Ptr->ConnHeadPtr = p_conn->NextPtr;
    } else {
        MQTTc_CONN  *p_iter_conn = MQTTc_Ptr->ConnHeadPtr;


                                                                /* Loop to find good conn.                              */
        while ((p_iter_conn->NextPtr != p_conn) &&
               (p_iter_conn->NextPtr != DEF_NULL)) {
            p_iter_conn = p_iter_conn->NextPtr;
        }
        if (p_iter_conn->NextPtr != DEF_NULL) {
            p_iter_conn->NextPtr = p_conn->NextPtr;
        } else {
            MQTTc_DBG_TRACE_INFO(("!!! ERROR !!! Could not find conn in conn list.\r\n"));
        }
    }
}
