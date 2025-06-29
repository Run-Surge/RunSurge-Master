#!/bin/bash

echo "🔧 Generating Python files from proto definitions..."

# Check if grpc_tools is installed
echo "🔍 Checking for grpc_tools..."
if ! python -m grpc_tools.protoc --help &> /dev/null; then
    echo "❌ Error: grpc_tools is not installed"
    echo ""
    echo "To install grpc_tools:"
    echo "  pip install grpcio-tools"
    exit 1
fi

output_folder="../protos"

echo "✓ grpc_tools found and working"

# Create output directory if it doesn't exist
mkdir -p $output_folder

# Proto files to generate
PROTO_FILES=("./common.proto" "./worker.proto" "./master.proto")

echo ""
echo "🔨 Generating Python files..."

success_count=0
total_files=${#PROTO_FILES[@]}

for proto_file in "${PROTO_FILES[@]}"; do
    if [ -f "$proto_file" ]; then
        echo ""
        echo "📝 Processing $proto_file..."
        
        # Generate Python bindings using grpc_tools
        if python -m grpc_tools.protoc -I. --python_out=$output_folder --pyi_out=$output_folder --grpc_python_out=$output_folder "$proto_file"; then
            echo "✓ Successfully generated $proto_file"
            ((success_count++))
        else
            echo "❌ Failed to generate $proto_file"
        fi
    else
        echo "⚠️  Warning: $proto_file not found"
    fi
done

# Create __init__.py file to make it a proper Python package
echo '"""Generated Protocol Buffer files."""' > $output_folder/__init__.py

echo ""
echo "✅ Generation complete!"
echo "📊 Successfully generated $success_count/$total_files files"

# List generated files
if [ "$(ls -A $output_folder/*.py 2>/dev/null)" ]; then
    echo ""
    echo "📋 Generated files:"
    for file in $output_folder/*.py; do
        if [ -f "$file" ]; then
            echo "   - $(basename "$file")"
        fi
    done
fi

echo ""
echo "📍 Files location: $(pwd)/$output_folder/" 