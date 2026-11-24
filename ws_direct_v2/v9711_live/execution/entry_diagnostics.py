"""Attribute missed entries from exact evidence without changing execution decisions."""
from ..exchange.binance_futures_trade_ws import APIError
from .submit_errors import (ConfirmedSubmitRejection, EntryQuoteUnavailable,
                            ExchangeSubmitRejected, ExchangeSubmitResponseError,
                            _exception_chain, is_agreement_rejection)


def submission_diagnostic(exc, *, rejected, disabled=False):
    error = ' '.join(str(exc).split())[:300] or type(exc).__name__
    reply = next((err for err in _exception_chain(exc)
                  if isinstance(err, (APIError, ExchangeSubmitRejected, ExchangeSubmitResponseError))), None)
    code = getattr(reply, 'code', None)
    detail = {'submit_error': error}
    if isinstance(code, int) and not isinstance(code, bool):
        detail['submit_error_code'] = code
    elif isinstance(code, str) and len(code) <= 12 and code.lstrip('-').isdigit():
        detail['submit_error_code'] = code
    agreement = rejected and is_agreement_rejection(exc)
    if rejected and (reply is not None or agreement):
        category = 'exchange_rejected'
        if agreement:
            detail['submit_error_reason'] = 'tradfi_agreement_required'
    elif reply is None and (disabled or (rejected and isinstance(exc, ConfirmedSubmitRejection))
                             or error in ('trade broker rate limit cooldown; no request sent',
                                          'trade WS reconnect cooldown; no request sent')):
        category = 'local_not_sent'
        if isinstance(exc, EntryQuoteUnavailable):
            detail['submit_error_reason'] = 'account_gate' if exc.retry_gate == 'account' else 'quote_gate'
    else:
        category = 'result_unknown' if not rejected else 'rejected_source_unknown'
    return dict(detail, submit_error_category=category)


LABELS = {
    'exchange_rejected': '交易所拒单',
    'local_not_sent': '本地拦截（未发单）',
    'result_unknown': '提交结果待核对',
    'rejected_source_unknown': '已确认拒绝，来源未确定',
    'resting_unfilled': '交易所已接单，尚未成交',
    'ended_unfilled': '交易所已接单，终结时未成交',
    'awaiting_result': '已发送，等待确认',
    'awaiting_send': '已登记，未见发送记录',
    'no_evidence': '未见匹配委托，原因未记录',
}
REASONS = {
    'tradfi_agreement_required': '账户未签署 TradFi 合约协议',
    'quote_gate': '发单前报价未通过检查',
    'account_gate': '发单前账户状态未就绪',
    'signal_follow_agreement_rejected_for_position': '本笔补挂已被交易所拒绝：账户未签署 TradFi 合约协议',
    'signal_follow_position_cap': '实际持仓已达补挂上限',
    'signal_follow_order_cap': '补挂名额已满',
    'signal_follow_capacity_full': '补挂容量已满',
    'signal_follow_symbol_already_held': 'Trade 已持有同一币对',
    'signal_follow_retry_wait': '补挂等待重试',
    'signal_follow_no_valid_quote': '补挂没有可用报价',
    'position_or_pending_capacity_full': '持仓或普通挂单名额已满',
    'account_not_ready_before_registration': '账户状态未就绪',
    'order_bar_expired_before_submit': '入场 bar 已过期',
    'admission_quote_expired': '入场报价过期',
    'admission_quote_not_qualified': '入场报价未达到价差门槛',
    'post_close_reopen_cooldown': '平仓后冷却中',
    'exact_client_identity_already_used_or_plan_rejected': '委托身份已使用或该计划此前被拒绝',
}


def unfilled_diagnosis(attempts):
    latest, skip = attempts.get('latest') or {}, attempts.get('last_skip')
    status = latest.get('status')
    error_category = latest.get('submit_error_category')
    if status == 'REJECTED':
        return 'exchange_rejected'
    if status == 'UNKNOWN':
        return error_category or 'result_unknown'
    if status == 'SUBMIT_REJECTED':
        return error_category or 'rejected_source_unknown'
    if latest.get('order_id') and status in {'CANCELED', 'CANCELLED', 'EXPIRED', 'EXPIRED_IN_MATCH'}:
        return 'ended_unfilled'
    if latest.get('order_id') and status == 'NEW':
        return 'resting_unfilled'
    if latest.get('send_epoch_ms') is not None:
        return 'awaiting_result'
    if latest:
        return 'awaiting_send'
    if skip:
        return 'local_not_sent'
    return 'no_evidence'
