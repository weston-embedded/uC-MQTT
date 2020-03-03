/*
*********************************************************************************************************
*                                            EXAMPLE CODE
*
*               This file is provided as an example on how to use Micrium products.
*
*               Please feel free to use any application code labeled as 'EXAMPLE CODE' in
*               your application products.  Example code may be used as is, in whole or in
*               part, or may be used as a reference only. This file can be modified as
*               required to meet the end-product requirements.
*
*********************************************************************************************************
*/

/*
*********************************************************************************************************
*
*                                           MQTTc APPLICATION
*
* Filename : app_mqtt-c_echo.c
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

#define    APP_MQTTc_ECHO_MODULE

#include  <cpu.h>
#include  <lib_def.h>

#include  "app_mqtt-c.h"

#include  <Source/dns-c.h>

#include  <Source/net.h>
#include  <Source/net_sock.h>
#include  <Source/net_util.h>
#include  <Source/net_ascii.h>

#include  <Source/os.h>

#include  <dns-c_cfg.h>

#include  <stdio.h>


/*
*********************************************************************************************************
*********************************************************************************************************
*                                            LOCAL DEFINES
*********************************************************************************************************
*********************************************************************************************************
*/

#define  APP_MQTTc_MSG_QTY                         3u
#define  APP_MQTTc_MSG_LEN_MAX                  1024u
#define  APP_MQTTc_PAYLOAD_LEN_MAX                64u

#define  APP_MQTTc_DOMAIN_PUBLISH_STATUS            "domain/status"
#define  APP_MQTTc_DOMAIN_PUBLISH_STATUS_QoS       2u

#define  APP_MQTTc_DOMAIN_PUBLISH_ECHO              "domain/echo"
#define  APP_MQTTc_DOMAIN_PUBLISH_ECHO_QoS         0u

#define  APP_MQTTc_DOMAIN_SUBSCRIBE_LISTEN          "domain/listen"
#define  APP_MQTTc_DOMAIN_SUBSCRIBE_LISTEN_QoS     1u


/*
*********************************************************************************************************
*********************************************************************************************************
*                                       LOCAL GLOBAL VARIABLES
*********************************************************************************************************
*********************************************************************************************************
*/

static  CPU_INT08U   AppMQTTc_TaskStk[APP_MQTTc_TASK_STK_SIZE];

static  MQTTc_CONN   AppMQTTc_Conn;
static  MQTTc_MSG    AppMQTTc_StatusMsg;
static  MQTTc_MSG    AppMQTTc_EchoMsg;
static  MQTTc_MSG    AppMQTTc_ListenRxMsg;
static  CPU_INT08U   AppMQTTc_BufTbl[APP_MQTTc_MSG_QTY][APP_MQTTc_MSG_LEN_MAX];
static  CPU_CHAR     AppMQTTc_Payload[APP_MQTTc_PAYLOAD_LEN_MAX];

static  CPU_BOOLEAN  App_MQTTc_StatusMsgIsAvail;
static  CPU_BOOLEAN  App_MQTTc_EchoMsgIsAvail;


const  NET_TASK_CFG  AppMQTTc_TaskCfg = {                       /* Cfg for MQTTc internal task.                         */
    APP_MQTTc_TASK_PRIO,                                        /* MQTTc internal task prio.                            */
    APP_MQTTc_TASK_STK_SIZE,                                    /* MQTTc internal task stack size.                      */
    AppMQTTc_TaskStk                                            /* Ptr to start of MQTTc internal stack.                */
};


const  MQTTc_CFG     AppMQTTc_Cfg = {
    APP_MQTTc_MSG_QTY,
    APP_MQTTc_INACTIVITY_TIMEOUT_s,
    APP_MQTTc_INTERNAL_TASK_DLY
};


/*
*********************************************************************************************************
*********************************************************************************************************
*                                      LOCAL FUNCTION PROTOTYPES
*********************************************************************************************************
*********************************************************************************************************
*/

static  void  AppMQTTc_OnCmplCallbackFnct         (       MQTTc_CONN  *p_conn,
                                                          MQTTc_MSG   *p_msg,
                                                          void        *p_arg,
                                                          MQTTc_ERR    err);

static  void  AppMQTTc_OnConnectCmplCallbackFnct  (       MQTTc_CONN  *p_conn,
                                                          MQTTc_MSG   *p_msg,
                                                          void        *p_arg,
                                                          MQTTc_ERR    err);

static  void  AppMQTTc_OnPublishCmplCallbackFnct  (       MQTTc_CONN  *p_conn,
                                                          MQTTc_MSG   *p_msg,
                                                          void        *p_arg,
                                                          MQTTc_ERR    err);

static  void  AppMQTTc_OnSubscribeCmplCallbackFnct(       MQTTc_CONN  *p_conn,
                                                             MQTTc_MSG   *p_msg,
                                                             void        *p_arg,
                                                             MQTTc_ERR    err);

static  void  AppMQTTc_OnPublishRxCallbackFnct    (       MQTTc_CONN  *p_conn,
                                                   const  CPU_CHAR    *topic_name_str,
                                                          CPU_INT32U   topic_len,
                                                   const  CPU_CHAR    *p_payload,
                                                          CPU_INT32U   payload_len,
                                                          void        *p_arg,
                                                          MQTTc_ERR    err);

static  void  AppMQTTc_OnErrCallbackFnct          (       MQTTc_CONN  *p_conn,
                                                          void        *p_arg,
                                                          MQTTc_ERR    err);


/*
*********************************************************************************************************
*                                            AppMQTTc_Init()
*
* Description : Initialize the MQTT-client module.
*
* Arguments   : none.
*
* Return(s)   : DEF_OK,   if NO error(s),
*               DEF_FAIL, otherwise.
*
* Caller(s)   : Application.
*
* Note(s)     : none.
*********************************************************************************************************
*/

CPU_BOOLEAN  AppMQTTc_Init (void)
{
    MQTTc_ERR  err_mqttc;


    App_MQTTc_StatusMsgIsAvail = DEF_YES;
    App_MQTTc_EchoMsgIsAvail   = DEF_YES;

    MQTTc_Init(&AppMQTTc_Cfg,
               &AppMQTTc_TaskCfg,
                DEF_NULL,
               &err_mqttc);
    if (err_mqttc != MQTTc_ERR_NONE) {
        printf("!!! APP ERROR !!! Failed to init MQTTc module. Err: %i\n\r.", err_mqttc);
        return (DEF_FAIL);
    }

    MQTTc_MsgClr(&AppMQTTc_StatusMsg, &err_mqttc);
    MQTTc_MsgSetParam(&AppMQTTc_StatusMsg, MQTTc_PARAM_TYPE_MSG_BUF_PTR, (void *)&AppMQTTc_BufTbl[0u],   &err_mqttc);
    MQTTc_MsgSetParam(&AppMQTTc_StatusMsg, MQTTc_PARAM_TYPE_MSG_BUF_LEN, (void *) APP_MQTTc_MSG_LEN_MAX, &err_mqttc);

    MQTTc_MsgClr(&AppMQTTc_EchoMsg, &err_mqttc);
    MQTTc_MsgSetParam(&AppMQTTc_EchoMsg, MQTTc_PARAM_TYPE_MSG_BUF_PTR, (void *)&AppMQTTc_BufTbl[1u],   &err_mqttc);
    MQTTc_MsgSetParam(&AppMQTTc_EchoMsg, MQTTc_PARAM_TYPE_MSG_BUF_LEN, (void *) APP_MQTTc_MSG_LEN_MAX, &err_mqttc);

    MQTTc_MsgClr(&AppMQTTc_ListenRxMsg, &err_mqttc);
    MQTTc_MsgSetParam(&AppMQTTc_ListenRxMsg, MQTTc_PARAM_TYPE_MSG_BUF_PTR, (void *)&AppMQTTc_BufTbl[2u],   &err_mqttc);
    MQTTc_MsgSetParam(&AppMQTTc_ListenRxMsg, MQTTc_PARAM_TYPE_MSG_BUF_LEN, (void *) APP_MQTTc_MSG_LEN_MAX, &err_mqttc);


    MQTTc_ConnClr(&AppMQTTc_Conn,
                  &err_mqttc);
    if (err_mqttc != MQTTc_ERR_NONE) {
        printf("!!! APP ERROR !!! Failed to clr MQTTc connection object. Err: %i\n\r.", err_mqttc);
        return (DEF_FAIL);
    }

                                                                /* Err handling should be done, in your application.    */
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_BROKER_NAME,                  (void *) APP_MQTTc_BROKER_NAME,                  &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_CLIENT_ID_STR,                (void *)"App_MQTT_TestClientID",                 &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_USERNAME_STR,                 (void *) APP_MQTTc_USERNAME,                     &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_PASSWORD_STR,                 (void *) APP_MQTTc_PASSWORD,                     &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_KEEP_ALIVE_TMR_SEC,           (void *) 1000u,                                  &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_CALLBACK_ON_COMPL,            (void *) AppMQTTc_OnCmplCallbackFnct,            &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_CALLBACK_ON_CONNECT_CMPL,     (void *) AppMQTTc_OnConnectCmplCallbackFnct,     &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_CALLBACK_ON_PUBLISH_CMPL,     (void *) AppMQTTc_OnPublishCmplCallbackFnct,     &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_CALLBACK_ON_SUBSCRIBE_CMPL,   (void *) AppMQTTc_OnSubscribeCmplCallbackFnct,   &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_CALLBACK_ON_PUBLISH_RX,       (void *) AppMQTTc_OnPublishRxCallbackFnct,       &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_CALLBACK_ON_ERR_CALLBACK,     (void *) AppMQTTc_OnErrCallbackFnct,             &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_TIMEOUT_MS,                   (void *) 30000u,                                 &err_mqttc);
    MQTTc_ConnSetParam(&AppMQTTc_Conn, MQTTc_PARAM_TYPE_PUBLISH_RX_MSG_PTR,           (void *)&AppMQTTc_ListenRxMsg,                   &err_mqttc);

    printf("Done setting params.\r\n");

    MQTTc_ConnOpen(&AppMQTTc_Conn,                              /* Open conn to MQTT server with parameters set in Conn.*/
                    MQTTc_FLAGS_NONE,
                   &err_mqttc);
    if (err_mqttc != MQTTc_ERR_NONE) {
        printf("!!! APP ERROR !!! Failed to open TCP connection to MQTT server. Err: %i\n\r.", err_mqttc);
        return (DEF_FAIL);                                      /* Failed to open TCP connection to MQTT server.        */
    }
    printf("Done opening conn.\r\n");

    App_MQTTc_StatusMsgIsAvail = DEF_NO;
    MQTTc_Connect(&AppMQTTc_Conn,                               /* Send CONNECT msg to MQTT server.                     */
                  &AppMQTTc_StatusMsg,
                  &err_mqttc);
    if (err_mqttc != MQTTc_ERR_NONE) {
        printf("!!! APP ERROR !!! Failed to process Connect msg req. Err: %i\n\r.", err_mqttc);
        return (DEF_FAIL);                                      /* Failed to process MQTT CONNECT msg.                  */
    }
    printf("Done calling MQTTc_Connect().\r\n");

    return (DEF_OK);
}


/*
*********************************************************************************************************
*                                     AppMQTTc_OnCmplCallbackFnct()
*
* Description : Generic callback function for MQTTc module.
*
* Arguments   : p_conn          Pointer to MQTTc Connection object for which operation has completed.
*
*               p_msg           Pointer to MQTTc Message object used for operation.
*
*               p_arg           Pointer to argument set in MQTTc Connection using the parameter type
*                               MQTTc_PARAM_TYPE_CALLBACK_ARG_PTR.
*
*               err             Error code from processing message.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc module.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  AppMQTTc_OnCmplCallbackFnct (MQTTc_CONN  *p_conn,
                                           MQTTc_MSG   *p_msg,
                                           void        *p_arg,
                                           MQTTc_ERR    err)
{
    (void)&p_conn;
    (void)&p_arg;

    if (err != MQTTc_ERR_NONE) {
        printf("Operation completed with err (%i). ", err);
    }

    switch (p_msg->Type) {
        case MQTTc_MSG_TYPE_CONNECT:                            /* Gen callback called for event type: Connect Cmpl.    */
             printf("Gen callback called for event type: Connect Cmpl.\n\r");
             break;


        case MQTTc_MSG_TYPE_PUBLISH:                            /* Gen callback called for event type: Publish Cmpl.    */
             printf("Gen callback called for event type: Publish Cmpl.\n\r");
             break;


        case MQTTc_MSG_TYPE_SUBSCRIBE:                          /* Gen callback called for event type: Subscribe Cmpl.  */
             printf("Gen callback called for event type: Subscribe Cmpl.\n\r");
             break;


        case MQTTc_MSG_TYPE_UNSUBSCRIBE:                        /* Gen callback called for event type: Unsubscribe Cmpl.*/
             printf("Gen callback called for event type: Unsubscribe Cmpl.\n\r");
             break;


        case MQTTc_MSG_TYPE_PINGREQ:                            /* Gen callback called for event type: PingReq Cmpl.    */
             printf("Gen callback called for event type: PingReq Cmpl.\n\r");
             break;


        case MQTTc_MSG_TYPE_DISCONNECT:                         /* Gen callback called for event type: Disconnect Cmpl. */
             printf("Gen callback called for event type: Disconnect Cmpl.\n\r");
             break;


        default:
             printf("Gen callback called for event type: default. !!! ERROR !!! %i\n\r", p_msg->Type);
             break;
    }
}


/*
*********************************************************************************************************
*                                 AppMQTTc_OnConnectCmplCallbackFnct()
*
* Description : Callback function for MQTTc module called when a CONNECT operation has completed.
*
* Arguments   : p_conn          Pointer to MQTTc Connection object for which operation has completed.
*
*               p_msg           Pointer to MQTTc Message object used for operation.
*
*               p_arg           Pointer to argument set in MQTTc Connection using the parameter type
*                               MQTTc_PARAM_TYPE_CALLBACK_ARG_PTR.
*
*               err             Error code from processing CONNECT message.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc module.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  AppMQTTc_OnConnectCmplCallbackFnct (MQTTc_CONN  *p_conn,
                                                  MQTTc_MSG   *p_msg,
                                                  void        *p_arg,
                                                  MQTTc_ERR    err)
{
    (void)&p_arg;

    if (err != MQTTc_ERR_NONE) {
        printf("ConnectCmpl callback called with err (%i).\n\r", err);
    } else {
        printf("ConnectCmpl callback called without err, ready to send/receive messages.\n\r");

        MQTTc_Subscribe(p_conn,
                        p_msg,                                  /* Re-using msg used by completed CONNECT msg.          */
                        APP_MQTTc_DOMAIN_SUBSCRIBE_LISTEN,
                        APP_MQTTc_DOMAIN_SUBSCRIBE_LISTEN_QoS,
                       &err);
        if (err != MQTTc_ERR_NONE) {
            printf("!!! APP ERROR !!! Subscribe failed. Err: %i\n\r.", err);
        }
    }
}


/*
*********************************************************************************************************
*                                 AppMQTTc_OnPublishCmplCallbackFnct()
*
* Description : Callback function for MQTTc module called when a PUBLISH operation has completed.
*
* Arguments   : p_conn          Pointer to MQTTc Connection object for which operation has completed.
*
*               p_msg           Pointer to MQTTc Message object used for operation.
*
*               p_arg           Pointer to argument set in MQTTc Connection using the parameter type
*                               MQTTc_PARAM_TYPE_CALLBACK_ARG_PTR.
*
*               err             Error code from processing PUBLISH message.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc module.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  AppMQTTc_OnPublishCmplCallbackFnct (MQTTc_CONN  *p_conn,
                                                  MQTTc_MSG   *p_msg,
                                                  void        *p_arg,
                                                  MQTTc_ERR    err)
{
    (void)&p_conn;
    (void)&p_arg;

    if (err != MQTTc_ERR_NONE) {
        printf("PublishCmpl callback called with error (%i). CANNOT continue.\n\r", err);
    } else {
        if (p_msg == &AppMQTTc_StatusMsg) {
            printf("PublishCmpl callback called for status. Marking message as available.\n\r");
            App_MQTTc_StatusMsgIsAvail = DEF_YES;               /* Mark msg as re-available.                            */
        } else {
            printf("PublishCmpl callback called for status. Marking message as available.\n\r");
            App_MQTTc_EchoMsgIsAvail = DEF_YES;                 /* Mark msg as re-available.                            */
        }
    }
}


/*
*********************************************************************************************************
*                                AppMQTTc_OnSubscribeCmplCallbackFnct()
*
* Description : Callback function for MQTTc module called when a SUBSCRIBE operation has completed.
*
* Arguments   : p_conn          Pointer to MQTTc Connection object for which operation has completed.
*
*               p_msg           Pointer to MQTTc Message object used for operation.
*
*               p_arg           Pointer to argument set in MQTTc Connection using the parameter type
*                               MQTTc_PARAM_TYPE_CALLBACK_ARG_PTR.
*
*               err             Error code from processing SUBSCRIBE message.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc module.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  AppMQTTc_OnSubscribeCmplCallbackFnct (MQTTc_CONN  *p_conn,
                                                    MQTTc_MSG   *p_msg,
                                                    void        *p_arg,
                                                    MQTTc_ERR    err)
{
    CPU_INT16U   payload_len;
    CPU_CHAR    *p_payload = &AppMQTTc_Payload[0];


    (void)&p_arg;

    if (err != MQTTc_ERR_NONE) {
        printf("SubscribeCmpl callback called with error (%i). CANNOT continue.\n\r", err);
    } else {
        printf("SubscribeCmpl callback called. Publishing status.\n\r");

        Str_Copy(p_payload,                                     /* Copy the string to publish to the payload buffer     */
                "Now listening on specified topic");
        payload_len = Str_Len(p_payload);                       /* Determine the length of the string we're publishing  */

        MQTTc_Publish(p_conn,
                      p_msg,                                    /* Re-using msg used by completed SUBSCRIBE msg.        */
                      APP_MQTTc_DOMAIN_PUBLISH_STATUS,
                      APP_MQTTc_DOMAIN_PUBLISH_STATUS_QoS,
                      DEF_NO,
                      p_payload,
                      payload_len,
                     &err);
        if (err != MQTTc_ERR_NONE) {
            printf("!!! APP ERROR !!! Failed to Publish Status. Err: %i\n\r.", err);
        }
    }
}


/*
*********************************************************************************************************
*                                  AppMQTTc_OnPublishRxCallbackFnct()
*
* Description : Callback function for MQTTc module called when a PUBLISH message has been received.
*
* Arguments   : p_conn          Pointer to MQTTc Connection object for which operation has completed.
*
*               topic_name_str  String containing the topic of the message received. NOT NULL-terminated.
*
*               topic_len       Length of the topic.
*
*               p_payload       NULL-terminated buffer containing the payload received.
*
*               payload_len     Length of the received payload
*
*               p_arg           Pointer to argument set in MQTTc Connection using the parameter type
*                               MQTTc_PARAM_TYPE_CALLBACK_ARG_PTR.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc module.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  AppMQTTc_OnPublishRxCallbackFnct (       MQTTc_CONN  *p_conn,
                                                const  CPU_CHAR    *topic_name_str,
                                                       CPU_INT32U   topic_len,
                                                const  CPU_CHAR    *p_payload,
                                                       CPU_INT32U   payload_len,
                                                       void        *p_arg,
                                                       MQTTc_ERR    err)
{
    CPU_INT16U   payload_len;
    CPU_CHAR    *p_payload = &AppMQTTc_Payload[0];


    (void)&p_arg;

    if (err != MQTTc_ERR_NONE) {
        printf("!!! APP ERROR !!! Err detected when receiving a PUBLISH message (%i). NOT echoing.\n\r", err);
        return;
    }

    printf("Received PUBLISH message from server. Topic is %.*s.", topic_len, topic_name_str);
    printf(" Message is %s.\n\r", p_payload);

    if (App_MQTTc_EchoMsgIsAvail == DEF_YES) {
        App_MQTTc_EchoMsgIsAvail = DEF_NO;
        MQTTc_Publish(p_conn,
                     &AppMQTTc_EchoMsg,
                      APP_MQTTc_DOMAIN_PUBLISH_STATUS,
                      APP_MQTTc_DOMAIN_PUBLISH_STATUS_QoS,
                      DEF_NO,
                      p_payload,
                      Str_Len(p_payload),
                     &err);
        if (err != MQTTc_ERR_NONE) {
            printf("!!! APP ERROR !!! Failed to Echo received message. Err: %i\n\r.", err);
        }
    } else if (App_MQTTc_StatusMsgIsAvail == DEF_YES) {
        App_MQTTc_StatusMsgIsAvail = DEF_NO;

        Str_Copy(p_payload,                                      /* Copy the string to publish to the payload buffer     */
                "Unable to send echo msg: msg unavailable.");
        payload_len = Str_Len(p_payload);                        /* Determine the length of the string we're publishing  */

        MQTTc_Publish(p_conn,
                     &AppMQTTc_StatusMsg,
                      APP_MQTTc_DOMAIN_PUBLISH_STATUS,
                      APP_MQTTc_DOMAIN_PUBLISH_STATUS_QoS,
                      DEF_NO,
                      p_payload,
                      payload_len,
                     &err);
        if (err != MQTTc_ERR_NONE) {
            printf("!!! APP ERROR !!! Failed to Publish Status. Err: %i\n\r.", err);
        }
    } else {
        printf("!!! APP ERROR !!! Echo and Status messages are both unavailable. Cannot send either Echo or Status to broker.\r\n");
    }
}


/*
*********************************************************************************************************
*                                     AppMQTTc_OnErrCallbackFnct()
*
* Description : Callback function for MQTTc module called when an error occurs.
*
* Arguments   : p_conn          Pointer to MQTTc Connection object on which error occurred.
*
*               p_arg           Pointer to argument set in MQTTc Connection using the parameter type
*                               MQTTc_PARAM_TYPE_CALLBACK_ARG_PTR.
*
*               err             Error code.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc module.
*
* Note(s)     : none.
*********************************************************************************************************
*/

static  void  AppMQTTc_OnErrCallbackFnct (MQTTc_CONN  *p_conn,
                                          void        *p_arg,
                                          MQTTc_ERR    err)
{
    CPU_INT16U   payload_len;
    CPU_CHAR    *p_payload = &AppMQTTc_Payload[0];


    (void)&p_conn;
    (void)&p_arg;

    printf("!!! APP ERROR !!! Err detected via OnErr callback. Err = %i.", err);

    if (App_MQTTc_StatusMsgIsAvail == DEF_YES) {
        App_MQTTc_StatusMsgIsAvail = DEF_NO;

        Str_Copy(p_payload,                                     /* Copy the string to publish to the payload buffer     */
                "Err detected");
        payload_len = Str_Len(p_payload);                       /* Determine the length of the string we're publishing  */

        printf("Sending status.\r\n");
        MQTTc_Publish(p_conn,
                     &AppMQTTc_StatusMsg,
                      APP_MQTTc_DOMAIN_PUBLISH_STATUS,
                      APP_MQTTc_DOMAIN_PUBLISH_STATUS_QoS,
                      DEF_NO,
                      p_payload,
                      payload_len,
                     &err);
        if (err != MQTTc_ERR_NONE) {
            printf("!!! APP ERROR !!! Failed to Publish Status. Err: %i\n\r.", err);
        }
    } else {
        printf("Unable to send status, message is not available.\r\n");
    }
}
