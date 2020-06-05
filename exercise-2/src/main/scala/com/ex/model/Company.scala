package com.ex.model

case class Company (
                    source_id: Int,
                    company_id: Int,
                    attribute_id: Int,
                    attribute_value: String,
                    attribute_prob: Double,
                    batch_id: Int
                    )
