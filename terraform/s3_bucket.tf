# Criação do bucket S3
resource "aws_s3_bucket" "meu_bucket_s3" {
  bucket = "meu-bucket-s3-unique-name"  # Substitua pelo nome único do seu bucket
  
  # Configuração de tags (opcional)
  tags = {
    Name = "Meu Bucket S3"
    Environment = "Production"
  }
}

# Upload do arquivo .parquet para o bucket S3
resource "aws_s3_bucket_object" "arquivo_parquet" {
  bucket = aws_s3_bucket.meu_bucket_s3.bucket
  key    = "estabelecimentos_gov"  # Nome do arquivo no bucket
  
  # Caminho local para o arquivo .parquet
  source = "/caminho/local/para/seu/arquivo.parquet"
  
  # ACL do objeto no bucket
  acl    = "private"  # Ajuste as permissões conforme necessário
  
  # Dependência do upload do arquivo para criação do bucket
  depends_on = [aws_s3_bucket.meu_bucket_s3]
}
