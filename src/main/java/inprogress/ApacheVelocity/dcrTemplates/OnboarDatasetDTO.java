package inprogress.ApacheVelocity.dcrTemplates;

public class OnboarDatasetDTO {
    Boolean isOwner;
    OnboarDatasetDTO(Boolean isOwner){
        this.isOwner = isOwner;
    }
    public boolean getIsOwner() {
        return this.isOwner;
    }
}
