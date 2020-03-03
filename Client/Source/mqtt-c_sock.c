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
* Filename : mqtt-c_sock.c
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
#include  <lib_def.h>
#include  <Source/net_sock.h>
#include  <Source/net_app.h>
#include  "mqtt-c_sock.h"


/*
*********************************************************************************************************
*********************************************************************************************************
*                                            LOCAL DEFINES
*********************************************************************************************************
*********************************************************************************************************
*/

#define  MQTTc_SOCK_SEL_FLAG_DESC_MSK              (DEF_BIT_00 | DEF_BIT_01 | DEF_BIT_02)
#define  MQTTc_SOCK_SEL_FLAG_DESC_RD                DEF_BIT_00
#define  MQTTc_SOCK_SEL_FLAG_DESC_WR                DEF_BIT_01
#define  MQTTc_SOCK_SEL_FLAG_DESC_ERR               DEF_BIT_02

#define  MQTTc_NET_SOCK_SEL_TIMEOUT_us                    1000u


/*
*********************************************************************************************************
*********************************************************************************************************
*                                       LOCAL GLOBAL VARIABLES
*********************************************************************************************************
*********************************************************************************************************
*/

static  NET_SOCK_DESC  MQTTc_NetSockDescRd;
static  NET_SOCK_DESC  MQTTc_NetSockDescWr;
static  NET_SOCK_DESC  MQTTc_NetSockDescErr;

static  NET_SOCK_TIMEOUT  MQTTc_NetSockSelTimeout = {
    0u,
    MQTTc_NET_SOCK_SEL_TIMEOUT_us
};


/*
*********************************************************************************************************
*********************************************************************************************************
*                                            GLOBAL FUNCTIONS
*********************************************************************************************************
*********************************************************************************************************
*/

/*
*********************************************************************************************************
*                                         MQTTc_SockConnOpen()
*
* Description : Open socket for MQTTc module.
*
* Argument(s) : p_conn      Pointer to MQTTc_CONN to open.
*
*               p_err       Pointer to variable that will receive error code from this function:
*                               MQTTc_ERR_NONE          Socket operation completed successfully.
*                               MQTTc_ERR_NULL_PTR      Parameter was null.
*                               MQTTc_ERR_SOCK_FAIL     Socket operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_SockConnOpen().
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_SockConnOpen (MQTTc_CONN  *p_conn,
                          MQTTc_ERR   *p_err)
{
    NET_ERR      err_net;
    CPU_BOOLEAN  flag    = DEF_TRUE;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }

        if (p_conn->BrokerNamePtr == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
    #endif

    NetApp_ClientStreamOpenByHostname(&p_conn->SockId,
                                       p_conn->BrokerNamePtr,
                                       p_conn->BrokerPortNbr,
                                       DEF_NULL,
                                       p_conn->SecureCfgPtr,
                                       p_conn->TimeoutMs,
                                      &err_net);
    switch (err_net) {
        case NET_APP_ERR_NONE:
             break;

        default:
             p_conn->SockId = NET_SOCK_ID_NONE;
            *p_err = MQTTc_ERR_SOCK_FAIL;
             return;
    }

    NetSock_CfgBlock(p_conn->SockId, NET_SOCK_BLOCK_SEL_NO_BLOCK, &err_net);
    if (err_net != NET_SOCK_ERR_NONE) {
        goto end_err;
    }

    (void)NetSock_OptSet(         p_conn->SockId,               /* Set NO DELAY option.                                 */
                                  NET_SOCK_PROTOCOL_TCP,
                                  NET_SOCK_OPT_TCP_NO_DELAY,
                         (void *)&flag,
                                  sizeof(flag),
                                 &err_net);
    if (err_net != NET_SOCK_ERR_NONE) {
        goto end_err;
    }

                                                                /* Set sock conn inactivity timeout.                    */
    (void)NetSock_OptSet(         p_conn->SockId,
                                  NET_SOCK_PROTOCOL_TCP,
                                  NET_SOCK_OPT_TCP_KEEP_IDLE,
                         (void *)&p_conn->InactivityTimeout_s,
                                  sizeof(p_conn->InactivityTimeout_s),
                                 &err_net);
    if (err_net != NET_SOCK_ERR_NONE) {
        goto end_err;
    }

   *p_err = MQTTc_ERR_NONE;

    return;

end_err:
   *p_err = MQTTc_ERR_SOCK_FAIL;

    NetSock_Close(p_conn->SockId, &err_net);
    (void)&err_net;

    p_conn->SockId = NET_SOCK_ID_NONE;

    return;
}


/*
*********************************************************************************************************
*                                         MQTTc_SockConnClose()
*
* Description : Close socket for MQTTc module.
*
* Argument(s) : p_conn      Pointer to MQTTc_CONN to close.
*
*               p_err       Pointer to variable that will receive error code from this function:
*                               MQTTc_ERR_NONE          Socket operation completed successfully.
*                               MQTTc_ERR_NULL_PTR      Parameter was null.
*                               MQTTc_ERR_SOCK_FAIL     Socket operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_SockConnClose().
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_SockConnClose (MQTTc_CONN  *p_conn,
                           MQTTc_ERR   *p_err)
{
    NET_ERR  err_net;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return;
        }
    #endif

    NetSock_Close(p_conn->SockId, &err_net);
    if (err_net == NET_SOCK_ERR_NONE) {
       *p_err = MQTTc_ERR_NONE;
    } else {
        *p_err = MQTTc_ERR_FAIL;
    }
}


/*
*********************************************************************************************************
*                                            MQTTc_SockTx()
*
* Description : Transmit data on given connection's socket.
*
* Argument(s) : p_conn      Pointer to MQTTc_CONN that needs to transmit.
*
*               p_buf       Pointer to start of buffer to transmit.
*
*               buf_len     Length, in bytes, to transmit.
*
*               p_err       Pointer to variable that will receive error code from this function:
*                               MQTTc_ERR_NONE          Socket operation completed successfully.
*                               MQTTc_ERR_NULL_PTR      Parameter was null.
*                               MQTTc_ERR_TX            Transmit operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_WrSockProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

CPU_INT32U  MQTTc_SockTx (MQTTc_CONN  *p_conn,
                          CPU_INT08U  *p_buf,
                          CPU_INT32U   buf_len,
                          MQTTc_ERR   *p_err)
{
    NET_SOCK_RTN_CODE  ret_val;
    NET_ERR            err_net;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return (0u);
        }

        if (p_buf == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return (0u);
        }
    #endif

    ret_val = NetSock_TxData(        p_conn->SockId,
                             (void *)p_buf,
                                     buf_len,
                                     NET_SOCK_FLAG_TX_NO_BLOCK,
                                    &err_net);
    if (err_net == NET_SOCK_ERR_NONE) {
       *p_err = MQTTc_ERR_NONE;
    } else {
       *p_err   = MQTTc_ERR_TX;
        ret_val = 0u;
    }

    return (ret_val);
}


/*
*********************************************************************************************************
*                                            MQTTc_SockRx()
*
* Description : Receive data on given connection's socket.
*
* Argument(s) : p_conn      Pointer to MQTTc_CONN that needs to receive.
*
*               p_buf       Pointer to start of buffer in which received data will be put.
*
*               buf_len     Length, in bytes, of receive buffer.
*
*               p_err       Pointer to variable that will receive error code from this function:
*                               MQTTc_ERR_NONE          Socket operation completed successfully.
*                               MQTTc_ERR_NULL_PTR      Parameter was null.
*                               MQTTc_ERR_RX_BUF_EMPTY  No more bytes available to receive at the moment.
*                               MQTTc_ERR_RX            Receive operation failed.
*                               MQTTc_ERR_FATAL         Fatal err reported.
*
* Return(s)   : Number of bytes received, if NO error(s),
*               0,                        otherwise.
*
* Caller(s)   : MQTTc_RdSockProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

CPU_INT32U  MQTTc_SockRx (MQTTc_CONN  *p_conn,
                          CPU_INT08U  *p_buf,
                          CPU_INT32U   buf_len,
                          MQTTc_ERR   *p_err)
{
    NET_SOCK_RTN_CODE  ret_val;
    NET_ERR            err_net;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }

        if (p_conn == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return (0u);
        }

        if (p_buf == DEF_NULL) {
           *p_err = MQTTc_ERR_NULL_PTR;
            return (0u);
        }
    #endif

    ret_val = NetSock_RxData(p_conn->SockId,
                             p_buf,
                             buf_len,
                             NET_SOCK_FLAG_NONE,
                            &err_net);
    switch (err_net) {
        case NET_SOCK_ERR_NONE:
            *p_err = MQTTc_ERR_NONE;
             break;

        case NET_SOCK_ERR_RX_Q_EMPTY:
            *p_err = MQTTc_ERR_RX_BUF_EMPTY;
             break;

        case NET_ERR_RX:
            *p_err   = MQTTc_ERR_RX;
             ret_val = 0u;
             break;

        default:
            *p_err   = MQTTc_ERR_FATAL;
             ret_val = 0u;
             break;
    }

    return (ret_val);
}


/*
*********************************************************************************************************
*                                        MQTTc_SockSelDescSet()
*
* Description : Set select descriptor type for given connection.
*
* Argument(s) : p_conn          Pointer to MQTTc_CONN for which to set its descriptor.
*
*               sel_desc_type   Select descriptor type to set.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_MsgProcess(),
*               MQTTc_WrSockProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_SockSelDescSet (MQTTc_CONN           *p_conn,
                            MQTTc_SEL_DESC_TYPE   sel_desc_type)
{
    NET_ERR  err_net;


    switch (sel_desc_type) {
        case MQTTc_SEL_DESC_TYPE_RD:
             DEF_BIT_SET(p_conn->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_RD);
             break;

        case MQTTc_SEL_DESC_TYPE_WR:
             DEF_BIT_SET(p_conn->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_WR);
             break;

        case MQTTc_SEL_DESC_TYPE_ERR:
        default:
             DEF_BIT_SET(p_conn->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_ERR);
             break;
    }

    NetSock_SelAbort(p_conn->SockId, &err_net);
    (void)&err_net;

    return;
}


/*
*********************************************************************************************************
*                                        MQTTc_SockSelDescClr()
*
* Description : Clear select descriptor type for given connection.
*
* Argument(s) : p_conn          Pointer to MQTTc_CONN for which to clear its descriptor.
*
*               sel_desc_type   Select descriptor type to clear.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_WrSockProcess().
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_SockSelDescClr (MQTTc_CONN           *p_conn,
                            MQTTc_SEL_DESC_TYPE   sel_desc_type)
{
    NET_ERR  err_net;


    switch (sel_desc_type) {
        case MQTTc_SEL_DESC_TYPE_RD:
             DEF_BIT_CLR(p_conn->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_RD);
             break;

        case MQTTc_SEL_DESC_TYPE_WR:
             DEF_BIT_CLR(p_conn->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_WR);
             break;

        case MQTTc_SEL_DESC_TYPE_ERR:
        default:
             DEF_BIT_CLR(p_conn->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_ERR);
             break;
    }

    NetSock_SelAbort(p_conn->SockId, &err_net);
    (void)&err_net;

    return;
}


/*
*********************************************************************************************************
*                                        MQTTc_SockSelDescProc()
*
* Description : Process select descriptor type for given connection.
*
* Argument(s) : p_conn          Pointer to MQTTc_CONN on which to process select descriptor.
*
*               sel_desc_type   Select descriptor type to process.
*
* Return(s)   : DEF_YES, if given descriptor is set,
*               DEF_NO,  otherwise.
*
* Caller(s)   : MQTTc_Task().
*
* Note(s)     : none.
*********************************************************************************************************
*/

CPU_BOOLEAN  MQTTc_SockSelDescProc (MQTTc_CONN           *p_conn,
                                    MQTTc_SEL_DESC_TYPE   sel_desc_type)
{
    NET_SOCK_DESC  *p_net_sock_desc;
    CPU_BOOLEAN     ret_val         = DEF_NO;


    switch (sel_desc_type) {
        case MQTTc_SEL_DESC_TYPE_RD:
             p_net_sock_desc = &MQTTc_NetSockDescRd;
             break;

        case MQTTc_SEL_DESC_TYPE_WR:
             p_net_sock_desc = &MQTTc_NetSockDescWr;
             break;

        case MQTTc_SEL_DESC_TYPE_ERR:
        default:
             p_net_sock_desc = &MQTTc_NetSockDescErr;
             break;
    }

    ret_val = NET_SOCK_DESC_IS_SET(p_conn->SockId, p_net_sock_desc);

    return (ret_val);
}


/*
*********************************************************************************************************
*                                            MQTTc_SockSel()
*
* Description : Execute select operation for selected connections.
*
* Argument(s) : p_head_conn Pointer to head of MQTTc Connection object list.
*
*               p_err       Pointer to variable that will receive error code from this function:
*                               MQTTc_ERR_NONE          Socket operation completed successfully.
*                               MQTTc_ERR_TIMEOUT       Operation timed-out.
*                               MQTTc_ERR_SOCK_FAIL     Socket operation failed.
*
* Return(s)   : none.
*
* Caller(s)   : MQTTc_Task().
*
* Note(s)     : none.
*********************************************************************************************************
*/

void  MQTTc_SockSel (MQTTc_CONN  *p_head_conn,
                     MQTTc_ERR   *p_err)
{
    MQTTc_CONN   *p_conn_iter;
    CPU_BOOLEAN   must_call_sel = DEF_NO;
    NET_ERR       err_net;


    #if (MQTTc_CFG_ARG_CHK_EXT_EN == DEF_ENABLED)
        if (p_err == DEF_NULL) {
            CPU_SW_EXCEPTION(;);
        }
    #endif

    NET_SOCK_DESC_INIT(&MQTTc_NetSockDescRd);
    NET_SOCK_DESC_INIT(&MQTTc_NetSockDescWr);
    NET_SOCK_DESC_INIT(&MQTTc_NetSockDescErr);

    p_conn_iter = p_head_conn;
    while (p_conn_iter != DEF_NULL) {
        if ((p_conn_iter->SockId                                                         != NET_SOCK_ID_NONE) &&
            (DEF_BIT_IS_SET_ANY(p_conn_iter->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_MSK) == DEF_YES)) {
            must_call_sel = DEF_YES;
            if (DEF_BIT_IS_SET(p_conn_iter->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_RD) == DEF_YES) {
                NET_SOCK_DESC_SET(p_conn_iter->SockId, &MQTTc_NetSockDescRd);
            }
            if (DEF_BIT_IS_SET(p_conn_iter->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_WR) == DEF_YES) {
                NET_SOCK_DESC_SET(p_conn_iter->SockId, &MQTTc_NetSockDescWr);
            }
            if (DEF_BIT_IS_SET(p_conn_iter->SockSelFlags, MQTTc_SOCK_SEL_FLAG_DESC_ERR) == DEF_YES) {
                NET_SOCK_DESC_SET(p_conn_iter->SockId, &MQTTc_NetSockDescErr);
            }
        }
        p_conn_iter = p_conn_iter->NextPtr;
    }

    if (must_call_sel == DEF_YES) {
        (void)NetSock_Sel(NET_SOCK_NBR_SOCK,
                         &MQTTc_NetSockDescRd,
                         &MQTTc_NetSockDescWr,
                         &MQTTc_NetSockDescErr,
                         &MQTTc_NetSockSelTimeout,
                         &err_net);
        switch (err_net) {
            case NET_SOCK_ERR_NONE:
                *p_err = MQTTc_ERR_NONE;
                 break;

            case NET_SOCK_ERR_TIMEOUT:
                *p_err = MQTTc_ERR_TIMEOUT;
                 break;

            default:
                *p_err = MQTTc_ERR_SOCK_FAIL;
                 break;
        }
    } else {
       *p_err = MQTTc_ERR_NONE;
    }

    return;
}
