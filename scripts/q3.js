var results = db.events.aggregate([
    {
        $match: {
            $or: [
                { category_code: { $regex: "electronics", $options: "i" } },
                { category_code: { $regex: "clothing", $options: "i" } },
                { category_code: { $regex: "home", $options: "i" } }
            ]
        }
    },
    {
        $group: {
            _id: { 
                product_id: "$product_id", 
                category_code: "$category_code",
                brand: "$brand"
            },
            price: { $first: "$price" },
            event_count: { $sum: 1 },
            avg_price: { $avg: "$price" }
        }
    },
    { $sort: { event_count: -1 } },
    { $limit: 50 },
    {
        $project: {
            product_id: "$_id.product_id",
            category_code: "$_id.category_code",
            brand: "$_id.brand",
            price: 1,
            event_count: 1,
            avg_price: 1
        }
    }
]);
printjson(results);
