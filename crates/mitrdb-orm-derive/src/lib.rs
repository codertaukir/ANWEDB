//! `#[derive(Model)]` for mitrdb-orm.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, parse_macro_input};

#[proc_macro_derive(Model, attributes(model, col))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(t) => t.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "Model does not support generic structs",
        ));
    }
    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    name,
                    "Model needs a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "Model can only be derived for structs",
            ));
        }
    };

    let mut table = format!("{}s", name.to_string().to_lowercase());
    for attr in &input.attrs {
        if attr.path().is_ident("model") {
            attr.parse_nested_meta(|m| {
                if m.path.is_ident("table") {
                    let v: LitStr = m.value()?.parse()?;
                    table = v.value();
                    Ok(())
                } else {
                    Err(m.error("unknown #[model(..)] option, expected: table = \"name\""))
                }
            })?;
        }
    }

    let mut metas = Vec::new();
    let mut to_row = Vec::new();
    let mut from_row = Vec::new();
    let mut handles = Vec::new();
    let mut pk_count = 0;

    for f in fields {
        let ident = f.ident.as_ref().unwrap();
        let fname = ident.to_string().trim_start_matches("r#").to_string();
        let ty = &f.ty;
        let (mut pk, mut uq, mut ix) = (false, false, false);
        for attr in &f.attrs {
            if attr.path().is_ident("col") {
                attr.parse_nested_meta(|m| {
                    if m.path.is_ident("primary_key") {
                        pk = true;
                    } else if m.path.is_ident("unique") {
                        uq = true;
                    } else if m.path.is_ident("index") {
                        ix = true;
                    } else {
                        return Err(m.error(
                            "unknown #[col(..)] option, expected: primary_key, unique, index",
                        ));
                    }
                    Ok(())
                })?;
            }
        }
        if pk {
            pk_count += 1;
            if pk_count > 1 {
                return Err(syn::Error::new_spanned(
                    ident,
                    "only one primary_key column is supported",
                ));
            }
        }
        let mut meta = quote!(::mitrdb_orm::ColumnMeta::of::<#ty>(#fname));
        if pk {
            meta = quote!(#meta.primary_key());
        }
        if uq {
            meta = quote!(#meta.unique());
        }
        if ix {
            meta = quote!(#meta.index());
        }
        metas.push(meta);
        to_row.push(quote!((#fname, ::mitrdb_orm::Field::to_value(&self.#ident))));
        from_row.push(quote!(#ident: row.get::<#ty>(#fname)?));
        handles.push(quote! {
            pub const #ident: ::mitrdb_orm::Col<#name, #ty> = ::mitrdb_orm::Col::new(#fname);
        });
    }

    Ok(quote! {
        #[allow(non_upper_case_globals)]
        impl #name {
            #(#handles)*
        }

        impl ::mitrdb_orm::Model for #name {
            const TABLE: &'static str = #table;
            const COLUMNS: &'static [::mitrdb_orm::ColumnMeta] = &[ #(#metas),* ];
            fn to_row(&self) -> ::std::vec::Vec<(&'static str, ::mitrdb_orm::Value)> {
                ::std::vec![ #(#to_row),* ]
            }
            fn from_row(row: &::mitrdb_orm::RowRef) -> ::mitrdb_orm::Result<Self> {
                Ok(#name { #(#from_row),* })
            }
        }
    })
}
