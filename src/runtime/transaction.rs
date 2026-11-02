//! Content-bound RPC transactions: body values travel inline and all wide
//! integers use the canonical decimal-string contract of TransactionPayload.swift.
use lattice_primitives::*;
use serde::{Deserialize, Deserializer};
use std::{collections::BTreeMap, str::FromStr};
fn decimal<'de, D: Deserializer<'de>, T: FromStr + ToString>(d: D) -> Result<T, D::Error> {
    let text = String::deserialize(d)?;
    let value = text
        .parse::<T>()
        .map_err(|_| serde::de::Error::custom("decimal integer out of range"))?;
    if value.to_string() != text {
        return Err(serde::de::Error::custom("noncanonical decimal integer"));
    }
    Ok(value)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Account {
    owner: String,
    #[serde(deserialize_with = "decimal")]
    delta: i64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Deposit {
    #[serde(deserialize_with = "decimal")]
    nonce: u128,
    demander: String,
    #[serde(deserialize_with = "decimal")]
    amount_demanded: u64,
    #[serde(deserialize_with = "decimal")]
    amount_deposited: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Receipt {
    withdrawer: String,
    #[serde(deserialize_with = "decimal")]
    nonce: u128,
    demander: String,
    #[serde(deserialize_with = "decimal")]
    amount_demanded: u64,
    directory: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Withdrawal {
    withdrawer: String,
    #[serde(deserialize_with = "decimal")]
    nonce: u128,
    demander: String,
    #[serde(deserialize_with = "decimal")]
    amount_demanded: u64,
    #[serde(deserialize_with = "decimal")]
    amount_withdrawn: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Body {
    account_actions: Vec<Account>,
    actions: Vec<Action>,
    deposit_actions: Vec<Deposit>,
    receipt_actions: Vec<Receipt>,
    withdrawal_actions: Vec<Withdrawal>,
    signers: Vec<String>,
    #[serde(deserialize_with = "decimal")]
    nonce: u64,
    chain_path: Vec<String>,
}
#[derive(Deserialize)]
struct Payload {
    signatures: BTreeMap<String, String>,
    body: Body,
}
pub fn decode(value: serde_json::Value) -> Result<Transaction, String> {
    let payload: Payload = serde_json::from_value(value).map_err(|e| e.to_string())?;
    let b = payload.body;
    let body = TransactionBody {
        account_actions: b
            .account_actions
            .into_iter()
            .map(|a| AccountAction {
                owner: a.owner,
                delta: a.delta,
            })
            .collect(),
        actions: b.actions,
        deposit_actions: b
            .deposit_actions
            .into_iter()
            .map(|a| DepositAction {
                nonce: a.nonce,
                demander: a.demander,
                amount_demanded: a.amount_demanded,
                amount_deposited: a.amount_deposited,
            })
            .collect(),
        receipt_actions: b
            .receipt_actions
            .into_iter()
            .map(|a| ReceiptAction {
                withdrawer: a.withdrawer,
                nonce: a.nonce,
                demander: a.demander,
                amount_demanded: a.amount_demanded,
                directory: a.directory,
            })
            .collect(),
        withdrawal_actions: b
            .withdrawal_actions
            .into_iter()
            .map(|a| WithdrawalAction {
                withdrawer: a.withdrawer,
                nonce: a.nonce,
                demander: a.demander,
                amount_demanded: a.amount_demanded,
                amount_withdrawn: a.amount_withdrawn,
            })
            .collect(),
        signers: b.signers,
        nonce: b.nonce,
        chain_path: b.chain_path,
    };
    Ok(Transaction::new(
        payload.signatures,
        lattice_cashew::Header::new(body).map_err(|e| e.to_string())?,
    ))
}
